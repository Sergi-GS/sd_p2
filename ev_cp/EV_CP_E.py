# ev_cp/EV_CP_E.py
import socket
import threading
import time
import json
import argparse
from kafka import KafkaConsumer, KafkaProducer

# --- Variables Globales de Estado ---
# Variable para simular el estado de salud del hardware/software
is_healthy = True
# Lock para proteger el acceso a 'is_healthy' desde múltiples hilos
health_lock = threading.Lock()

# Variable para almacenar el ID del CP (lo recibirá del Monitor)
cp_id_global = None
# Kafka Globals
kafka_producer = None
kafka_broker_global = None


def send_kafka_message(topic, message):
    """Función de ayuda para enviar mensajes a Kafka."""
    global kafka_producer
    try:
        if kafka_producer is None:
            print("[KAFKA_ERROR] El productor no está inicializado.")
            return
        
        kafka_producer.send(topic, message)
        kafka_producer.flush()
    except Exception as e:
        print(f"[KAFKA_ERROR] No se pudo enviar mensaje a {topic}: {e}")

def simulate_charging(driver_id, cp_id):
    """
    Simula el proceso de carga, enviando telemetría cada segundo.
    """
    print(f"🔌  Iniciando recarga para {driver_id} en {cp_id}...")
    
    # Simulación de una recarga de 10 segundos
    start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    total_kwh = 0
    total_euros = 0
    
    # Asumimos un precio fijo (la Central lo sabe, pero lo simulamos aquí)
    price_kwh = 0.50 
    
    for i in range(10):
        time.sleep(1)
        kwh_this_second = 0.5 # Simula 0.5 kWh por segundo
        total_kwh += kwh_this_second
        total_euros += kwh_this_second * price_kwh
        
        telemetry_data = {
            'cp_id': cp_id,
            'driver_id': driver_id,
            'status': 'SUMINISTRANDO',
            'kwh': round(total_kwh, 2),
            'euros': round(total_euros, 2),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Enviar telemetría a Central y al Driver
        send_kafka_message('topic_data_streaming', telemetry_data)
        print(f"  -> {total_kwh:.2f} kWh | {total_euros:.2f} €")

    end_time = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"✅  Recarga finalizada para {driver_id}.")
    
    # Enviar mensaje de finalización
    final_data = {
        'cp_id': cp_id,
        'driver_id': driver_id,
        'status': 'FINALIZADO',
        'start_time': start_time,
        'end_time': end_time,
        'total_kwh': round(total_kwh, 2),
        'total_euros': round(total_euros, 2)
    }
    send_kafka_message('topic_data_streaming', final_data)

def start_kafka_listener(cp_id, kafka_broker):
    """
    Inicia un consumidor de Kafka para este CP específico.
    Escucha en el tópico 'topic_commands_{cp_id}'
    """
    global kafka_producer, kafka_broker_global
    
    # Guardar globalmente para que el simulador de carga lo use
    kafka_broker_global = kafka_broker
    
    try:
        # Inicializar el productor que usará este CP
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Inicializar el consumidor
        command_topic = f'topic_commands_{cp_id}'
        consumer = KafkaConsumer(
            command_topic,
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        print(f"✅ [KAFKA] Oyente de Kafka conectado. Escuchando en '{command_topic}'")

        for msg in consumer:
            data = msg.value
            action = data.get('action')
            
            if action == 'START_CHARGE':
                driver_id = data.get('driver_id')
                
                # Lanzamos la simulación en un hilo para no bloquear al consumidor
                # Esto permite que el CP pueda recibir una orden de "STOP"
                # de Central mientras está cargando (lógica futura).
                charge_thread = threading.Thread(
                    target=simulate_charging, 
                    args=(driver_id, cp_id), 
                    daemon=True
                )
                charge_thread.start()
            
            # TODO: Implementar lógica para STOP_CP si viene de Central
            # (Aunque la lógica de STOP/RESUME la hemos puesto vía Sockets)

    except Exception as e:
        print(f"❌ [KAFKA_ERROR] Fallo fatal en el oyente de Kafka: {e}")

def handle_monitor_connection(conn, addr):
    """
    Maneja la conexión del EV_CP_M (Monitor).
    El Monitor es responsable de enviar pings de salud.
    Este Engine responde "OK" o "KO".
    """
    global cp_id_global
    print(f"🔌 [SOCKET] Monitor conectado desde: {addr}")
    
    try:
        # 1. El primer mensaje DEBE ser la identificación
        # Formato esperado: "ID;{cp_id}"
        id_data = conn.recv(1024).decode('utf-8').strip()
        if id_data.startswith('ID;'):
            cp_id = id_data.split(';')[1]
            cp_id_global = cp_id
            print(f"🆔 [SOCKET] Este Engine ha sido identificado como: {cp_id}")
            conn.sendall(b"ID_OK\n")
            
            # 2. Una vez identificado, iniciar el oyente de Kafka
            kafka_thread = threading.Thread(
                target=start_kafka_listener, 
                args=(cp_id, kafka_broker_global), 
                daemon=True
            )
            kafka_thread.start()
        
        else:
            print("❌ [SOCKET] El Monitor no envió un ID válido. Cerrando.")
            conn.sendall(b"ID_FAIL\n")
            return

        # 3. Bucle de Healthcheck (Ping/Pong)
        while True:
            # Espera el "PING" del Monitor
            data = conn.recv(1024)
            if not data:
                print("💔 [SOCKET] El Monitor se ha desconectado.")
                break
                
            # Comprobar el estado de salud
            with health_lock:
                current_health = is_healthy
            
            if current_health:
                conn.sendall(b"OK\n")
            else:
                conn.sendall(b"KO\n")
                
            time.sleep(0.1) # Pequeña pausa

    except (ConnectionResetError, BrokenPipeError):
        print(f"💔 [SOCKET] Conexión perdida con el Monitor {addr}")
    except Exception as e:
        print(f"❌ [SOCKET] Error en la conexión con el Monitor: {e}")
    finally:
        print("🔌 [SOCKET] Cerrando socket del Monitor.")
        conn.close()

def start_socket_server(port):
    """
    Inicia el servidor de Sockets que espera la conexión del Monitor.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind(('0.0.0.0', port))
        server.listen(1) # Solo esperamos 1 conexión: su Monitor
        print(f"✅ [SOCKET] Engine Server escuchando en el puerto {port}...")
        
        # Aceptar la conexión (bloqueante)
        conn, addr = server.accept()
        
        # Manejar la conexión en el hilo principal
        handle_monitor_connection(conn, addr)
        
    except Exception as e:
        print(f"❌ [SOCKET_ERROR] No se pudo iniciar el servidor en el puerto {port}: {e}")
    finally:
        server.close()

def failure_simulator():
    """
    Hilo que escucha la entrada del teclado para simular un fallo.
    """
    global is_healthy
    print("✅ [SIMULADOR] Simulador de averías iniciado.")
    print("   -> Presiona [Enter] en esta terminal para simular/resolver una avería (OK <-> KO)")
    
    while True:
        input() # Espera a que el usuario presione Enter
        
        with health_lock:
            is_healthy = not is_healthy # Invierte el estado de salud
            status = "BIEN ✅" if is_healthy else "AVERIADO ❌"
            print(f"\n🚨 [SIMULADOR] ¡Estado de salud cambiado! Ahora está: {status}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EV Charging Point - Engine")
    parser.add_argument('--socket-port', type=int, required=True, help="Puerto para el socket del Monitor")
    parser.add_argument('--kafka-broker', type=str, required=True, help="IP:Puerto del broker de Kafka")
    args = parser.parse_args()
    
    # Guardar el broker de Kafka globalmente para pasarlo al hilo de Kafka
    kafka_broker_global = args.kafka_broker

    # Iniciar el hilo para simular fallos
    fail_thread = threading.Thread(target=failure_simulator, daemon=True)
    fail_thread.start()

    # Iniciar el servidor de Sockets en el hilo principal
    start_socket_server(args.socket_port)