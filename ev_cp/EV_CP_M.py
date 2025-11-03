#!/usr/bin/env python3
# EV_CP_M.py - Monitor del Punto de Carga
import sys
import socket
import time
import json
from kafka import KafkaProducer

# Estado global del monitor
is_currently_broken = False

def print_help():
    print("Uso: python EV_CP_M.py <engine_ip> <engine_port> <kafka_bootstrap> <cp_id>")
    print("Ej: python EV_CP_M.py localhost 8001 localhost:9092 CP001")

def send_alert_to_central(bootstrap_server, cp_id, alert_type):
    """Envía una alerta (broken/recovered) a EV_Central vía Kafka."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        alert_msg = {
            "cp_id": cp_id,
            "alert": alert_type  # "broken" o "recovered"
        }
        producer.send("cp.monitor.alert", alert_msg)
        producer.flush()
        producer.close()
        status = "🔴 Averiado" if alert_type == "broken" else "🟢 Recuperado"
        print(f"[ALERTA ENVIADA] {status} - CP: {cp_id}")
    except Exception as e:
        print(f"[ERROR KAFKA] No se pudo enviar alerta: {e}")

def monitor_engine(engine_ip, engine_port, kafka_bootstrap, cp_id):
    """Bucle principal de monitorización del Engine."""
    global is_currently_broken

    while True:
        try:
            # Conectarse al Engine
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)  # Timeout de 3 segundos
            sock.connect((engine_ip, engine_port))
            print(f"🔌 Conectado al Engine en {engine_ip}:{engine_port}")

            while True:
                # Enviar mensaje de salud
                sock.send(b"HEALTH_CHECK\n")
                try:
                    response = sock.recv(1024).decode('utf-8').strip()
                    if response == "OK":
                        if is_currently_broken:
                            # ¡Se ha recuperado!
                            send_alert_to_central(kafka_bootstrap, cp_id, "recovered")
                            is_currently_broken = False
                        # print("[HEALTH] Engine OK")
                    elif response == "KO":
                        if not is_currently_broken:
                            send_alert_to_central(kafka_bootstrap, cp_id, "broken")
                            is_currently_broken = True
                    else:
                        raise Exception("Respuesta inesperada del Engine")

                except socket.timeout:
                    print("[HEALTH] Timeout: Engine no responde")
                    if not is_currently_broken:
                        send_alert_to_central(kafka_bootstrap, cp_id, "broken")
                        is_currently_broken = True

                time.sleep(1)  # Enviar check cada segundo

        except (ConnectionRefusedError, socket.timeout, OSError) as e:
            print(f"[ERROR SOCKET] No se puede conectar al Engine: {e}")
            if not is_currently_broken:
                send_alert_to_central(kafka_bootstrap, cp_id, "broken")
                is_currently_broken = True
            print("🔄 Reintentando conexión en 3 segundos...")
            time.sleep(3)
        except Exception as e:
            print(f"[ERROR] {e}")
            sock.close()
            time.sleep(1)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print_help()
        sys.exit(1)

    engine_ip = sys.argv[1]
    engine_port = int(sys.argv[2])
    kafka_bootstrap = sys.argv[3]
    cp_id = sys.argv[4]

    print(f"🚀 Monitor iniciado para CP '{cp_id}'")
    print(f"   → Engine: {engine_ip}:{engine_port}")
    print(f"   → Kafka: {kafka_bootstrap}")

    try:
        monitor_engine(engine_ip, engine_port, kafka_bootstrap, cp_id)
    except KeyboardInterrupt:
        print("\n🛑 Monitor detenido.")