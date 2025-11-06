# ev_driver/EV_Driver.py
import sys
import time
import json
import threading
import queue
from kafka import KafkaConsumer, KafkaProducer

# Importa la GUI que acabamos de crear
from driver_gui import DriverApp

class BackendController:
    """Maneja toda la lógica de Kafka (productor, consumidor, bucles) en hilos."""
    
    def __init__(self, gui_queue):
        self.gui_queue = gui_queue
        self.producer = None
        self.driver_id = None
        self.response_topic = None
        self.kafka_broker = None
        
        # Evento para sincronizar el bucle de servicios del archivo
        self.service_finished_event = threading.Event()

    def connect(self, driver_id, broker):
        """
        Llamado por la GUI. Inicializa el productor y lanza el hilo consumidor.
        """
        self.driver_id = driver_id
        self.kafka_broker = broker
        self.response_topic = f'topic_driver_{self.driver_id}'
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            self.gui_queue.put(("ADD_MESSAGE", "Productor Kafka OK."))
        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f"❌ Error conectando Productor: {e}"))
            return

        # Iniciar el hilo Consumidor
        consumer_thread = threading.Thread(
            target=self._start_kafka_listener,
            daemon=True
        )
        consumer_thread.start()
        time.sleep(1) # Dar tiempo al consumidor a suscribirse
        
    def _start_kafka_listener(self):
        """
        Inicia el consumidor. Escucha en 3 tópicos.
        (Esta es la lógica de tu 'start_kafka_listener' original, adaptada)
        """
        try:
            consumer = KafkaConsumer(
                self.response_topic,        # Tópico de respuesta de Central
                'topic_data_streaming',   # Tópico de telemetría de los Engines
                'topic_status_broadcast', # Tópico de estado de TODOS los CPs
                bootstrap_servers=self.kafka_broker,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            self.gui_queue.put(("ADD_MESSAGE", "Oyente Kafka OK. Escuchando..."))

            for msg in consumer:
                data = msg.value
                
                # --- Tópico 1: Respuesta de Central (Aprobado/Denegado) ---
                if msg.topic == self.response_topic:
                    status = data.get('status')
                    if status == 'APPROVED':
                        msg = f"✅ SOLICITUD APROBADA para {data.get('cp_id')}"
                        self.gui_queue.put(("ADD_MESSAGE", msg))
                        self.gui_queue.put(("ADD_MESSAGE", "...Esperando telemetría..."))
                    
                    elif status == 'DENIED':
                        msg = f"❌ SOLICITUD DENEGADA para {data.get('cp_id')}"
                        self.gui_queue.put(("ADD_MESSAGE", msg))
                        self.gui_queue.put(("ADD_MESSAGE", f"   Razón: {data.get('reason')}"))
                        self.service_finished_event.set() # Desbloquea el bucle de archivo

                # --- Tópico 2: Telemetría del Engine (Suministro/Finalizado) ---
                elif msg.topic == 'topic_data_streaming':
                    if data.get('driver_id') != self.driver_id:
                        continue # No es telemetría para nosotros
                    
                    status = data.get('status')
                    
                    if status == 'SUMINISTRANDO':
                        # Actualizar los labels de kWh y €
                        self.gui_queue.put(("UPDATE_CHARGE", data))
                    
                    elif status == 'FINALIZADO':
                        self.gui_queue.put(("ADD_MESSAGE", "\n" + "="*20))
                        self.gui_queue.put(("ADD_MESSAGE", f"✅ Recarga FINALIZADA."))
                        self.gui_queue.put(("ADD_MESSAGE", f"   Total: {data.get('total_kwh'):.2f} kWh"))
                        self.gui_queue.put(("ADD_MESSAGE", f"   Coste: {data.get('total_euros'):.2f} €"))
                        self.gui_queue.put(("ADD_MESSAGE", "="*20 + "\n"))
                        self.gui_queue.put(("RESET_CHARGE", None))
                        self.service_finished_event.set() # Desbloquea el bucle de archivo
                    
                    elif status == 'FINALIZADO_AVERIA':
                        self.gui_queue.put(("ADD_MESSAGE", "\n" + "="*20))
                        self.gui_queue.put(("ADD_MESSAGE", f"❌ Recarga INTERRUMPIDA POR AVERÍA."))
                        self.gui_queue.put(("ADD_MESSAGE", f"   Total: {data.get('total_kwh'):.2f} kWh"))
                        self.gui_queue.put(("ADD_MESSAGE", f"   Coste: {data.get('total_euros'):.2f} €"))
                        self.gui_queue.put(("ADD_MESSAGE", "="*20 + "\n"))
                        self.gui_queue.put(("RESET_CHARGE", None))
                        self.service_finished_event.set() # Desbloquea el bucle de archivo
                
                # --- Tópico 3: Estado de TODOS los CPs (para la lista) ---
                elif msg.topic == 'topic_status_broadcast':
                    self.gui_queue.put(("UPDATE_CP_LIST", data))

        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f"❌ Error fatal en oyente Kafka: {e}"))
            
    def _send_request(self, cp_id):
        """Función helper para enviar la solicitud de carga."""
        if not self.producer:
            self.gui_queue.put(("ADD_MESSAGE", "Error: Productor no conectado."))
            return
            
        request_payload = {
            'driver_id': self.driver_id,
            'cp_id': cp_id,
            'response_topic': self.response_topic
        }
        
        try:
            self.producer.send('topic_requests', request_payload)
            self.producer.flush()
            self.gui_queue.put(("ADD_MESSAGE", f"[{self.driver_id}] Solicitud para {cp_id} enviada..."))
        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f"❌ Error enviando solicitud: {e}"))
            
    # --- Funciones llamadas por la GUI ---
    
    def request_manual_service(self, cp_id):
        """Solicita una única carga manual."""
        self.gui_queue.put(("ADD_MESSAGE", "\n--- Nueva Petición Manual ---"))
        self._send_request(cp_id)
        
    def start_file_services(self, file_path):
        """Inicia el bucle de servicios de archivo en un nuevo hilo."""
        self.gui_queue.put(("ADD_MESSAGE", f"Iniciando servicios desde: {file_path}"))
        file_thread = threading.Thread(
            target=self._run_file_loop,
            args=(file_path,),
            daemon=True
        )
        file_thread.start()

    def _run_file_loop(self, file_path):
        """
        El bucle que lee el archivo y procesa los servicios uno por uno.
        (Esta es la lógica de tu 'main' original)
        """
        try:
            with open(file_path, 'r') as f:
                services = [line.strip() for line in f if line.strip()]
            self.gui_queue.put(("ADD_MESSAGE", f"Servicios a solicitar: {services}"))
        except Exception as e:
            self.gui_queue.put(("ADD_MESSAGE", f"❌ Error leyendo archivo: {e}"))
            return
            
        if not services:
            self.gui_queue.put(("ADD_MESSAGE", "Archivo de servicios vacío."))
            return

        # Bucle principal de servicios de archivo
        for cp_id in services:
            self.gui_queue.put(("ADD_MESSAGE", "\n" + "="*30))
            self.gui_queue.put(("ADD_MESSAGE", f"[{self.driver_id}] Solicitando servicio en: {cp_id}"))
            
            # Enviar la solicitud
            self._send_request(cp_id)

            # Esperar a que el servicio termine (DENIED, FINALIZADO, o AVERIA)
            self.service_finished_event.wait()
            
            # Limpiar el evento para el próximo servicio
            self.service_finished_event.clear()
            
            # Esperar