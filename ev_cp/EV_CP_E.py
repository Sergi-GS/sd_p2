#!/usr/bin/env python3
# EV_CP_E.py - Engine del Punto de Carga
import sys
import socket
import threading
import time
import json
import random
from kafka import KafkaProducer, KafkaConsumer

# Variables globales para el estado del CP
cp_id = None
location = "Madrid"
price_per_kwh = 0.45
current_status = "available"  # available, charging, broken, out_of_order
is_broken = False  # Para simular fallos manualmente
central_socket = None

# Kafka
kafka_bootstrap = None
producer = None

def print_help():
    print("Uso: python EV_CP_E.py <kafka_bootstrap> <monitor_ip> <monitor_port> [cp_id] [location] [price]")
    print("Ej: python EV_CP_E.py localhost:9092 localhost 8001 CP001 Madrid 0.45")

def connect_to_central(central_ip="localhost", central_port=8000):
    """Conecta al EV_Central via socket y se registra."""
    global central_socket, cp_id, location, price_per_kwh
    try:
        central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        central_socket.connect((central_ip, central_port))
        print(f"🔌 Conectado a EV_Central en {central_ip}:{central_port}")

        # Enviar mensaje de registro
        register_msg = f"REGISTER;{cp_id};{location};{price_per_kwh}\n"
        central_socket.send(register_msg.encode('utf-8'))
        response = central_socket.recv(1024).decode('utf-8').strip()
        print(f"✅ Registro confirmado por Central: {response}")
        return True
    except Exception as e:
        print(f"❌ Error al conectar con Central: {e}")
        return False

def handle_monitor_connection(conn, addr):
    """Maneja la conexión entrante del Monitor."""
    global is_broken
    print(f"[MONITOR] Conexión desde {addr}")
    try:
        while True:
            data = conn.recv(1024).decode('utf-8').strip()
            if not data:
                break
            if data == "HEALTH_CHECK":
                response = "KO" if is_broken else "OK"
                conn.send(f"{response}\n".encode('utf-8'))
                print(f"[MONITOR] Respondido: {response}")
    except Exception as e:
        print(f"[MONITOR] Error: {e}")
    finally:
        conn.close()

def start_monitor_server(monitor_port):
    """Inicia un servidor para que el Monitor se conecte."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', monitor_port))
    server.listen(1)
    print(f"📡 Servidor para Monitor iniciado en puerto {monitor_port}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_monitor_connection, args=(conn, addr), daemon=True).start()

def simulate_charging_session(driver_id):
    """Simula una sesión de carga: espera 'enchufado', envía telemetría."""
    global current_status, producer, cp_id
    print(f"\n⚡ Sesión de carga autorizada para conductor {driver_id}.")
    print("👉 Pulsa ENTER para simular que el vehículo se ha enchufado...")
    input()  # Espera interacción manual

    current_status = "charging"
    kwh = 0.0
    start_time = time.time()

    print("🔋 Suministro iniciado. Enviando telemetría cada segundo...")
    try:
        while True:
            # Simular consumo
            power_kw = round(random.uniform(3.0, 22.0), 2)
            kwh += power_kw / 3600  # kWh por segundo
            cost = kwh * price_per_kwh

            # Enviar telemetría a Central (Kafka)
            telemetry = {
                "cp_id": cp_id,
                "driver_id": driver_id,
                "power_kw": power_kw,
                "total_kwh": round(kwh, 3),
                "total_cost": round(cost, 2),
                "timestamp": time.time()
            }
            producer.send("cp.supply.update", telemetry)
            print(f"  [TELEMETRÍA] Potencia: {power_kw} kW | Total: {round(kwh, 3)} kWh | Coste: {round(cost, 2)} €")

            time.sleep(1)

            # Aquí podrías comprobar si el usuario quiere finalizar (ej. tecla 'q')
            # Para simplificar, finalizamos con Ctrl+C
    except KeyboardInterrupt:
        print("\n🛑 Suministro finalizado por el usuario.")
    finally:
        current_status = "available"
        # Notificar finalización a Central (Kafka)
        end_msg = {
            "cp_id": cp_id,
            "driver_id": driver_id,
            "event": "session_end",
            "total_kwh": round(kwh, 3),
            "total_cost": round(cost, 2)
        }
        producer.send("cp.supply.update", end_msg)

def kafka_listener():
    """Escucha comandos de la Central vía Kafka."""
    global cp_id
    consumer = KafkaConsumer(
        "central.command",
        bootstrap_servers=kafka_bootstrap,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        cmd = msg.value
        if cmd.get("cp_id") == cp_id:
            action = cmd.get("action")
            if action == "stop":
                print("🛑 Comando recibido: PARAR CP")
                # Aquí podrías detener el CP
            elif action == "resume":
                print("🟢 Comando recibido: REANUDAR CP")

def listen_for_driver_requests():
    """Escucha solicitudes de recarga de conductores."""
    global cp_id
    consumer = KafkaConsumer(
        "central.response",
        bootstrap_servers=kafka_bootstrap,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for msg in consumer:
        response = msg.value
        if response.get("cp_id") == cp_id and response.get("authorized"):
            driver_id = response.get("driver_id")
            simulate_charging_session(driver_id)

def simulate_failure():
    """Permite al usuario simular un fallo (responder KO al Monitor)."""
    global is_broken
    while True:
        print("\n🔧 [Engine] Pulsa 'f' para simular fallo, 'r' para recuperar:")
        cmd = input().strip().lower()
        if cmd == 'f':
            is_broken = True
            print("⚠️ Fallo simulado: responderé KO al Monitor.")
        elif cmd == 'r':
            is_broken = False
            print("✅ Recuperado: responderé OK al Monitor.")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print_help()
        sys.exit(1)

    kafka_bootstrap = sys.argv[1]  # Ej: localhost:9092
    monitor_ip = sys.argv[2]       # IP del Monitor (aunque el Engine escucha, no se usa aquí)
    monitor_port = int(sys.argv[3])

    # Parámetros opcionales
    cp_id = sys.argv[4] if len(sys.argv) > 4 else "CP001"
    location = sys.argv[5] if len(sys.argv) > 5 else "Madrid"
    price_per_kwh = float(sys.argv[6]) if len(sys.argv) > 6 else 0.45

    # Inicializar Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"🚀 Kafka listo en {kafka_bootstrap}")
    except Exception as e:
        print(f"❌ Error al conectar con Kafka: {e}")
        sys.exit(1)

    # Conectar a Central
    if not connect_to_central():
        sys.exit(1)

    # Iniciar servidor para Monitor
    threading.Thread(target=start_monitor_server, args=(monitor_port,), daemon=True).start()

    # Hilos para Kafka
    threading.Thread(target=kafka_listener, daemon=True).start()
    threading.Thread(target=listen_for_driver_requests, daemon=True).start()

    # Hilo para simular fallos
    threading.Thread(target=simulate_failure, daemon=True).start()

    print(f"\n✅ Engine del CP '{cp_id}' listo y registrado.")
    print("Esperando solicitudes de recarga...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Engine detenido.")
        if central_socket:
            central_socket.close()