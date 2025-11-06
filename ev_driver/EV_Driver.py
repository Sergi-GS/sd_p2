# ev_driver/EV_Driver.py
import sys
import time
import json
import threading
import argparse
import os
from kafka import KafkaConsumer, KafkaProducer

# Evento de sincronización para saber cuándo un servicio ha terminado
service_finished_event = threading.Event()
current_driver_id = None

# --- Variables Globales para Sincronización ---
active_cp_id = None  # El CP que estamos procesando AHORA
state_lock = threading.Lock() # Un lock para proteger 'active_cp_id'

def start_kafka_listener(driver_id, kafka_broker, response_topic):
    """
    Inicia el consumidor en un hilo separado.
    """
    global service_finished_event, active_cp_id, state_lock
    try:
        consumer = KafkaConsumer(
            response_topic,
            'topic_data_streaming',
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=f'driver_group_{driver_id}',
            auto_offset_reset='latest'
        )
        print(f"[{driver_id}] Oyente de Kafka conectado. Esperando mensajes...")

        for msg in consumer:
            data = msg.value
            msg_cp_id = data.get('cp_id')

            # --- LÓGICA DE SINCRONIZACIÓN ---
            with state_lock:
                if msg_cp_id != active_cp_id:
                    continue
            # --- FIN LÓGICA ---
            
            if msg.topic == response_topic:
                status = data.get('status')
                if status == 'APPROVED':
                    print(f"\n[{driver_id}] ✅ SOLICITUD APROBADA para {data.get('cp_id')}.")
                    print(f"[{driver_id}] ...Esperando inicio de telemetría...")
                
                elif status == 'DENIED':
                    print(f"\n[{driver_id}] ❌ SOLICITUD DENEGADA para {data.get('cp_id')}.")
                    print(f"[{driver_id}] Razón: {data.get('reason')}")
                    with state_lock:
                        active_cp_id = None
                    service_finished_event.set() 

            elif msg.topic == 'topic_data_streaming':
                if data.get('driver_id') != driver_id:
                    continue 
                
                status = data.get('status')
                
                if status == 'SUMINISTRANDO':
                    print(f"\r[{driver_id}] 🔌 Recargando... {data.get('kwh'):.2f} kWh | {data.get('euros'):.2f} €", end="")
                
                # --- LÓGICA DE FINALIZACIÓN MODIFICADA ---
                elif status in ('FINALIZADO', 'FINALIZADO_AVERIA', 'FINALIZADO_PARADA'):
                    if status == 'FINALIZADO':
                        print(f"\n[{driver_id}] ✅ Recarga FINALIZADA.")
                    elif status == 'FINALIZADO_AVERIA':
                        print(f"\n[{driver_id}] ❌ Recarga INTERRUMPIDA POR AVERÍA.")
                    elif status == 'FINALIZADO_PARADA':
                        print(f"\n[{driver_id}] 🛑 Recarga PARADA por la Central.")
                    
                    print(f"    Total: {data.get('total_kwh'):.2f} kWh")
                    print(f"    Coste: {data.get('total_euros'):.2f} €")
                    
                    with state_lock:
                        active_cp_id = None
                    service_finished_event.set()
                # --- FIN LÓGICA MODIFICADA ---

    except Exception as e:
        print(f"[{driver_id}] ❌ Error fatal en el oyente de Kafka: {e}")

def read_services_file(file_path):
    """Lee el archivo de servicios y devuelve una lista de CP_IDs."""
    try:
        with open(file_path, 'r') as f:
            services = [line.strip() for line in f if line.strip()]
            print(f"Servicios a solicitar: {services}")
            return services
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo de servicios en {file_path}")
        return []
    except Exception as e:
        print(f"❌ Error leyendo el archivo de servicios: {e}")
        return []

def load_driver_state(state_file):
    """Carga el índice del último servicio completado."""
    if not os.path.exists(state_file):
        return 0
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
            return state.get('next_service_index', 0)
    except Exception as e:
        print(f"[{current_driver_id}] Advertencia: No se pudo leer el archivo de estado {state_file}. Empezando de cero. Error: {e}")
        return 0

def save_driver_state(state_file, next_index):
    """Guarda el índice del PRÓXIMO servicio a ejecutar."""
    try:
        with open(state_file, 'w') as f:
            json.dump({'next_service_index': next_index}, f)
    except Exception as e:
        print(f"[{current_driver_id}] ❌ Error CRÍTICO: No se pudo guardar el estado en {state_file}. Error: {e}")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="EV Charging Point - Driver")
    parser.add_argument('--kafka-broker', required=True, help="IP:Puerto del broker de Kafka")
    parser.add_argument('--driver-id', required=True, help="ID único de este conductor")
    parser.add_argument('--file', required=True, help="Archivo .txt con la lista de servicios (CP_IDs)")
    args = parser.parse_args()
    
    current_driver_id = args.driver_id
    
    STATE_FILE = f"driver_state_{args.driver_id}.json"
    start_index = load_driver_state(STATE_FILE)
    
    services_to_request = read_services_file(args.file)
    if not services_to_request:
        print("No hay servicios que solicitar. Saliendo.")
        sys.exit(0)
    
    total_services = len(services_to_request)
    if start_index >= total_services:
        print(f"[{args.driver_id}] Todos los servicios ya estaban completados. Limpiando estado.")
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
        sys.exit(0)
    elif start_index > 0:
        print(f"[{args.driver_id}] Reanudando desde el servicio {start_index + 1} de {total_services}...")

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Error conectando el Productor de Kafka a {args.kafka_broker}: {e}")
        sys.exit(1)

    response_topic = f'topic_driver_{args.driver_id}'
    
    consumer_thread = threading.Thread(
        target=start_kafka_listener,
        args=(args.driver_id, args.kafka_broker, response_topic),
        daemon=True
    )
    consumer_thread.start()
    
    time.sleep(1) 

    # --- BUCLE PRINCIPAL CON MANEJO DE CTRL+C ---
    try:
        for i in range(start_index, total_services):
            cp_id = services_to_request[i]
            
            print("\n" + "="*30)
            print(f"[{args.driver_id}] Solicitando servicio {i+1}/{total_services} en: {cp_id}")
            
            with state_lock:
                active_cp_id = cp_id
            
            request_payload = {
                'driver_id': args.driver_id,
                'cp_id': cp_id,
                'response_topic': response_topic
            }
            
            try:
                producer.send('topic_requests', request_payload)
                producer.flush()
                print(f"[{args.driver_id}] Solicitud enviada. Esperando...")
            except Exception as e:
                print(f"[{args.driver_id}] ❌ Error enviando solicitud a Kafka: {e}")
                with state_lock:
                    active_cp_id = None
                continue 

            service_finished_event.wait()
            
            save_driver_state(STATE_FILE, i + 1)
            
            service_finished_event.clear()
            
            print(f"[{args.driver_id}] ...Esperando 4 segundos antes del siguiente servicio...")
            time.sleep(4)
            
    except KeyboardInterrupt:
        print(f"\n[{args.driver_id}] Saliendo... (El estado se ha guardado. Próximo servicio: {load_driver_state(STATE_FILE) + 1})")
        sys.exit(0)
    # --- FIN BUCLE MODIFICADO ---

    print("\n" + "="*30)
    print(f"[{args.driver_id}] Todos los servicios del archivo han sido procesados. Saliendo.")
    
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        
    producer.close()