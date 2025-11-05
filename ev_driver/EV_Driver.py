# ev_driver/EV_Driver.py
import sys
import time
import json
import threading
import argparse
from kafka import KafkaConsumer, KafkaProducer

# Evento de sincronización para saber cuándo un servicio ha terminado
service_finished_event = threading.Event()
current_driver_id = None

# EV_Driver.py (Reemplazar esta función)

def start_kafka_listener(driver_id, kafka_broker, response_topic):
    """
    Inicia el consumidor en un hilo separado.
    Escucha en dos tópicos:
    1. response_topic: Para saber si la solicitud fue APROBADA o DENEGADA.
    2. topic_data_streaming: Para recibir la telemetría en tiempo real.
    """
    global service_finished_event
    try:
        consumer = KafkaConsumer(
            response_topic,             # Tópico de respuesta de Central
            'topic_data_streaming',     # Tópico de telemetría de los Engines
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        print(f"[{driver_id}] Oyente de Kafka conectado. Esperando mensajes...")

        for msg in consumer:
            data = msg.value
            
            # --- Tópico 1: Respuesta de Central ---
            if msg.topic == response_topic:
                status = data.get('status')
                if status == 'APPROVED':
                    print(f"\n[{driver_id}] ✅ SOLICITUD APROBADA para {data.get('cp_id')}.")
                    print(f"[{driver_id}] ...Esperando inicio de telemetría...")
                
                elif status == 'DENIED':
                    print(f"\n[{driver_id}] ❌ SOLICITUD DENEGADA para {data.get('cp_id')}.")
                    print(f"[{driver_id}] Razón: {data.get('reason')}")
                    service_finished_event.set() # Desbloquea el hilo principal

            # --- Tópico 2: Telemetría del Engine ---
            elif msg.topic == 'topic_data_streaming':
                if data.get('driver_id') != driver_id:
                    continue
                
                status = data.get('status')
                
                if status == 'SUMINISTRANDO':
                    print(f"\r[{driver_id}] 🔌 Recargando... {data.get('kwh'):.2f} kWh | {data.get('euros'):.2f} €", end="")
                
                # --- LÓGICA DE FINALIZACIÓN MODIFICADA ---
                elif status == 'FINALIZADO':
                    print(f"\n[{driver_id}] ✅ Recarga FINALIZADA.")
                    print(f"    Total: {data.get('total_kwh'):.2f} kWh")
                    print(f"    Coste: {data.get('total_euros'):.2f} €")
                    service_finished_event.set() # Desbloquea el hilo principal
                
                elif status == 'FINALIZADO_AVERIA':
                    print(f"\n[{driver_id}] ❌ Recarga INTERRUMPIDA POR AVERÍA.")
                    print(f"    Total cargado: {data.get('total_kwh'):.2f} kWh")
                    print(f"    Coste: {data.get('total_euros'):.2f} €")
                    service_finished_event.set() # Desbloquea el hilo principal

    except Exception as e:
        print(f"[{driver_id}] ❌ Error fatal en el oyente de Kafka: {e}")

def read_services_file(file_path):
    """Lee el archivo de servicios y devuelve una lista de CP_IDs."""
    try:
        with open(file_path, 'r') as f:
            # Lee las líneas, quita espacios en blanco y filtra líneas vacías
            services = [line.strip() for line in f if line.strip()]
            print(f"Servicios a solicitar: {services}")
            return services
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo de servicios en {file_path}")
        return []
    except Exception as e:
        print(f"❌ Error leyendo el archivo de servicios: {e}")
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EV Charging Point - Driver")
    parser.add_argument('--kafka-broker', required=True, help="IP:Puerto del broker de Kafka")
    parser.add_argument('--driver-id', required=True, help="ID único de este conductor")
    parser.add_argument('--file', required=True, help="Archivo .txt con la lista de servicios (CP_IDs)")
    args = parser.parse_args()
    
    current_driver_id = args.driver_id
    
    # 1. Leer la lista de servicios del archivo
    services_to_request = read_services_file(args.file)
    if not services_to_request:
        print("No hay servicios que solicitar. Saliendo.")
        sys.exit(0)

    # 2. Inicializar el Productor de Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Error conectando el Productor de Kafka a {args.kafka_broker}: {e}")
        sys.exit(1)

    # 3. Definir el tópico de respuesta único para este driver
    response_topic = f'topic_driver_{args.driver_id}'
    
    # 4. Iniciar el hilo Consumidor
    consumer_thread = threading.Thread(
        target=start_kafka_listener,
        args=(args.driver_id, args.kafka_broker, response_topic),
        daemon=True
    )
    consumer_thread.start()
    
    time.sleep(1) # Dar tiempo al consumidor a suscribirse

    # 5. Bucle principal para solicitar servicios
    for cp_id in services_to_request:
        print("\n" + "="*30)
        print(f"[{args.driver_id}] Solicitando servicio en: {cp_id}")
        
        # a. Preparar el mensaje de solicitud
        request_payload = {
            'driver_id': args.driver_id,
            'cp_id': cp_id,
            'response_topic': response_topic # ¡Clave! Decirle a Central dónde responder
        }
        
        # b. Enviar la solicitud a Central
        try:
            producer.send('topic_requests', request_payload)
            producer.flush()
            print(f"[{args.driver_id}] Solicitud enviada. Esperando aprobación...")
        except Exception as e:
            print(f"[{args.driver_id}] ❌ Error enviando solicitud a Kafka: {e}")
            continue # Saltar al siguiente servicio

        # c. Esperar a que el servicio termine (el consumidor hará .set())
        service_finished_event.wait() # Bloquea el hilo principal
        
        # d. Limpiar el evento para el próximo servicio
        service_finished_event.clear()
        
        # e. Esperar 4 segundos antes del siguiente servicio
        print(f"[{args.driver_id}] ...Esperando 4 segundos antes del siguiente servicio...")
        time.sleep(4)

    print("\n" + "="*30)
    print(f"[{args.driver_id}] Todos los servicios del archivo han sido procesados. Saliendo.")
    producer.close()