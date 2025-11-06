# EV_Central.py
import sys
import time
import json
import threading
import sqlite3
import socket
import os
from kafka import KafkaConsumer, KafkaProducer
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.style import Style

DB_NAME = 'ev_central.db'
SOCKET_HOST = '0.0.0.0'
SOCKET_PORT = 8000
KAFKA_SERVER = 'localhost:9092'
HEARTBEAT_TIMEOUT = 15

#interfaz grafica en terminal
STATUS_COLORS = {
    'ACTIVADO': Style(color="green"),
    'SUMINISTRANDO': Style(color="green", bold=True), # Este es el "verde oscuro" para el parpadeo
    'AVERIADO': Style(color="red"),
    'PARADO': Style(color="yellow"),
    'DESCONECTADO': Style(color="grey50"),
}


#BDD
def get_db_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def update_cp_status_in_db(cp_id, new_status):
    """Actualiza el estado de un CP en la BBDD."""
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(
            "UPDATE ChargingPoints SET status = ?, last_update = CURRENT_TIMESTAMP WHERE cp_id = ?",
            (new_status, cp_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al actualizar estado: {e}")
    finally:
        if conn:
            conn.close()

def register_cp_in_db(cp_id, location, price):
    """
    Registra un CP si no existe.
    Si ya existe, NO actualiza sus datos (para mantener los manuales),
    solo actualiza su timestamp.
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO ChargingPoints (cp_id, location, price_kwh, status, last_heartbeat, last_update)
            VALUES (?, ?, ?, 'DESCONECTADO', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(cp_id) DO UPDATE SET
                -- No actualizamos la ubicación ni el precio,
                -- ¡mantenemos los que ya estaban en la BBDD!
                last_update = CURRENT_TIMESTAMP
            """,
            (cp_id, location, price)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al registrar CP: {e}")
    finally:
        if conn:
            conn.close()
            
def update_cp_heartbeat(cp_id):
    """Actualiza la marca de tiempo 'last_heartbeat' de un CP."""
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(
            "UPDATE ChargingPoints SET last_heartbeat = CURRENT_TIMESTAMP WHERE cp_id = ?",
            (cp_id,)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al actualizar heartbeat: {e}")
    finally:
        if conn:
            conn.close()

# --- FUNCIÓN CORREGIDA/AÑADIDA ---
def get_cp_info_from_db(cp_id):
    """Obtiene el estado y el precio de un CP."""
    conn = None
    info = {'status': None, 'price_kwh': 0.50} # Precio por defecto si algo falla
    try:
        conn = get_db_connection()
        cursor = conn.execute("SELECT status, price_kwh FROM ChargingPoints WHERE cp_id = ?", (cp_id,))
        result = cursor.fetchone()
        if result:
            info['status'] = result['status']
            info['price_kwh'] = result['price_kwh']
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al leer info de CP: {e}")
    finally:
        if conn:
            conn.close()
    return info
# --- FIN FUNCIÓN CORREGIDA ---


def broadcast_status_change(producer, cp_id, new_status, location=None, price=None):
    payload = {
        'cp_id': cp_id,
        'status': new_status,
    }
    if location:
        payload['location'] = location
    if price:
        payload['price_kwh'] = price
        
    producer.send('topic_status_broadcast', payload)
    producer.flush()

def send_socket_message(conn, message_str):
    """Envía un mensaje de texto simple con un salto de línea."""
    try:
        conn.sendall(f"{message_str}\n".encode('utf-8'))
    except (BrokenPipeError, ConnectionResetError):
        print(f"[SOCKET_SEND] Error: La conexión ya estaba cerrada.")


#sockets
def handle_socket_client(conn, addr, producer, active_connections, lock):
    print(f"[SOCKET] Nueva conexión del Monitor: {addr}")
    cp_id_autenticado = None
    try:
        while True:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                break 
            
            messages = data.strip().split('\n')
            for msg in messages:
                if not msg:
                    continue
                
                parts = msg.strip().split(';')
                command = parts[0]
                
                if command == 'REGISTER':
                    cp_id_autenticado = parts[1]
                    location = parts[2]
                    price = float(parts[3])
                    register_cp_in_db(cp_id_autenticado, location, price)
                    # Lo ponemos DESCONECTADO (el Monitor lo activará en 1 seg)
                    update_cp_status_in_db(cp_id_autenticado, "DESCONECTADO") 
                    
                    print(f"CP '{cp_id_autenticado}' REGISTRADO.")
                    conn.send(b"ACK;REGISTER_OK\n")
                    
                    with lock:
                        active_connections[cp_id_autenticado] = conn
                
                elif command == 'HEARTBEAT' and cp_id_autenticado:
                    update_cp_heartbeat(cp_id_autenticado)
                    conn.send(b"ACK;HEARTBEAT_OK\n")
                
                elif command == 'STATUS' and cp_id_autenticado:
                    new_status = parts[1]
                    update_cp_status_in_db(cp_id_autenticado, new_status)
                    broadcast_status_change(producer, cp_id_autenticado, new_status)
                    print(f"📡  Estado de '{cp_id_autenticado}' actualizado a {new_status} por Monitor.")
                    conn.send(b"ACK;STATUS_UPDATED\n")
                
    except (ConnectionResetError, BrokenPipeError):
        print(f"[SOCKET] Cliente {addr} desconectado abruptamente.")
    except Exception as e:
        print(f"[SOCKET] Error con {addr}: {e}")
    finally:
        if cp_id_autenticado:
            print(f"🔌 [SOCKET] Conexión cerrada. '{cp_id_autenticado}' pasa a DESCONECTADO.")
            update_cp_status_in_db(cp_id_autenticado, "DESCONECTADO")
            broadcast_status_change(producer, cp_id_autenticado, "DESCONECTADO")
            
            with lock:
                active_connections.pop(cp_id_autenticado, None)
            
        conn.close()

def start_socket_server(producer, active_connections, lock):
    """Inicia el servidor de Sockets concurrente para los Monitores."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SOCKET_HOST, SOCKET_PORT))
    server.listen(5)
    print(f"OK [CONTROL] Servidor de Sockets escuchando en {SOCKET_PORT}...")

    while True:
        conn, addr = server.accept()
        client_thread = threading.Thread(
            target=handle_socket_client, 
            args=(conn, addr, producer, active_connections, lock), 
            daemon=True
        )
        client_thread.start()

#kafka
def start_kafka_listener(producer, telemetry_lock, current_telemetry):
    """Inicia el consumidor/productor de Kafka para los Drivers y Engines."""
    consumer = KafkaConsumer(
        'topic_requests',
        'topic_data_streaming',
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    print(f"OK [DATOS] Oyente de Kafka conectado a {KAFKA_SERVER}...")

    for msg in consumer:
        try:
            data = msg.value
            
            if msg.topic == 'topic_requests':
                cp_id = data['cp_id']
                driver_id = data['driver_id']
                response_topic = data.get('response_topic')
                
                print(f" Petición de {driver_id} para {cp_id}")

                # 1. Obtener info (status Y precio) de la BBDD
                cp_info = get_cp_info_from_db(cp_id)
                status = cp_info['status']
                price_kwh = cp_info['price_kwh']
                
                if status == 'ACTIVADO':
                    update_cp_status_in_db(cp_id, 'SUMINISTRANDO')
                    print(f"OK  Petición APROBADA. Enviando orden a {cp_id} al precio de {price_kwh} €/kWh...")
                    
                    # 2. Enviar el precio al Engine en el mensaje
                    producer.send(
                        f'topic_commands_{cp_id}',
                        {
                            'action': 'START_CHARGE', 
                            'driver_id': driver_id,
                            'price_kwh': price_kwh
                        }
                    )
                    
                    broadcast_status_change(producer, cp_id, 'SUMINISTRANDO')
                    
                    if response_topic:
                        producer.send(
                            response_topic,
                            {'status': 'APPROVED', 'cp_id': cp_id, 'driver_id': driver_id}
                        )
                
                else:
                    print(f"  Petición DENEGADA. {cp_id} no está 'ACTIVADO' (estado: {status}).")
                    if response_topic:
                        try:
                            producer.send(
                                response_topic,
                                {'status': 'DENIED', 'cp_id': cp_id, 'driver_id': driver_id, 'reason': f"CP no disponible. Estado actual: {status}"}
                            )
                            producer.flush()
                        except Exception as e:
                            print(f"[KAFKA_ERROR] No se pudo enviar 'DENIED' a {response_topic}: {e}")

            elif msg.topic == 'topic_data_streaming':
                
                charge_status = data.get('status')
                cp_id = data.get('cp_id')
                
                if charge_status == 'SUMINISTRANDO':
                    with telemetry_lock:
                        current_telemetry[cp_id] = {
                            'kwh': data.get('kwh', 0),
                            'euros': data.get('euros', 0),
                            'driver_id': data.get('driver_id', 'N/A')
                        }
                
                # AÑADIDO 'FINALIZADO_PARADA'
                elif charge_status in ('FINALIZADO', 'FINALIZADO_AVERIA', 'FINALIZADO_PARADA'):
                    
                    # Guardar el log de la recarga
                    try:
                        conn = get_db_connection()
                        conn.execute(
                            """
                            INSERT INTO ChargeLog (cp_id, driver_id, start_time, end_time, total_kwh, total_euros)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (cp_id, data.get('driver_id'), data.get('start_time'), data.get('end_time'), data.get('total_kwh'), data.get('total_euros'))
                        )
                        conn.commit()
                        print(f"💾 Log de recarga guardado para {cp_id}.")
                    except sqlite3.Error as e:
                        print(f"[DB_ERROR] No se pudo guardar el ChargeLog: {e}")
                    finally:
                        if conn:
                            conn.close()

                    # Borrarlo del dict de telemetría
                    with telemetry_lock:
                        current_telemetry.pop(cp_id, None)

                    if charge_status == 'FINALIZADO':
                        print(f"✅  Carga finalizada en {cp_id}. Volviendo a ACTIVADO.")
                        update_cp_status_in_db(cp_id, 'ACTIVADO')
                        broadcast_status_change(producer, cp_id, 'ACTIVADO')
                    
                    elif charge_status == 'FINALIZADO_AVERIA':
                        print(f"❌  Carga interrumpida por avería en {cp_id}. El CP permanece AVERIADO.")
                        # (Correcto: el Monitor ya lo puso 'AVERIADO')
                        
                    elif charge_status == 'FINALIZADO_PARADA':
                        print(f"🛑  Carga interrumpida por parada admin en {cp_id}. El CP permanece PARADO.")
                        # (Correcto: el Admin ya lo puso 'PARADO')

        except json.JSONDecodeError:
            print(f"[KAFKA_ERROR] Mensaje malformado: {msg.value}")
        except Exception as e:
            print(f"[KAFKA_ERROR] Error procesando mensaje: {e}")

def central_command_input(producer, active_connections, lock):
    """Hilo que escucha comandos del admin en la terminal de Central."""
    print("✅ [ADMIN] Hilo de comandos iniciado. Escribe 'HELP' para ver opciones.")
    while True:
        try:
            cmd_line = input("> ")
            parts = cmd_line.strip().split()
            if not parts:
                continue

            command = parts[0].upper()
            
            if command == 'HELP':
                print("Comandos disponibles:")
                print("  STOP [CP_ID]   - Pone un CP en 'PARADO' (Out of Order)")
                print("  RESUME [CP_ID] - Vuelve a poner un CP en 'ACTIVADO'")
                
            elif command in ('STOP', 'RESUME') and len(parts) == 2:
                cp_id = parts[1]
                new_status = 'PARADO' if command == 'STOP' else 'ACTIVADO'
                
                target_conn = None
                with lock:
                    target_conn = active_connections.get(cp_id)
                
                if target_conn:
                    try:
                        cmd_to_send = "STOP_CP" if command == 'STOP' else "RESUME_CP"
                        send_socket_message(target_conn, cmd_to_send)
                        print(f"✅ [ADMIN] Comando '{cmd_to_send}' enviado a {cp_id}.")
                        
                        update_cp_status_in_db(cp_id, new_status)
                        broadcast_status_change(producer, cp_id, new_status)
                        
                    except Exception as e:
                        print(f"❌ [ADMIN] Error enviando comando a {cp_id}: {e}")
                else:
                    print(f"❌ [ADMIN] {cp_id} no está conectado (socket no encontrado).")

        except EOFError:
            break
        except Exception as e:
            print(f"❌ [ADMIN] Error en hilo de comandos: {e}")

# --- 3. Lógica de Resiliencia ---
def check_cp_heartbeats(producer):
    """Hilo de vigilancia que comprueba los 'last_heartbeat'."""
    print(f"✅ [RESILIENCIA] Vigilante de heartbeats iniciado (Timeout: {HEARTBEAT_TIMEOUT}s)...")
    while True:
        time.sleep(HEARTBEAT_TIMEOUT // 2)
        
        stale_cps = []
        conn_read = None
        try:
            conn_read = get_db_connection()
            cursor = conn_read.execute(
                """
                SELECT cp_id, status FROM ChargingPoints
                WHERE status != 'DESCONECTADO' 
                AND (STRFTIME('%s', 'now') - STRFTIME('%s', last_heartbeat)) > ?
                """,
                (HEARTBEAT_TIMEOUT,)
            )
            stale_cps = cursor.fetchall()
            
        except sqlite3.Error as e:
            print(f"[RESILIENCIA_ERROR] Error en BBDD: {e}")
        finally:
            if conn_read:
                conn_read.close()

        for row in stale_cps:
            cp_id = row['cp_id']
            print(f" [RESILIENCIA] No se recibió heartbeat de {cp_id}. Marcando como DESCONECTADO.")
            update_cp_status_in_db(cp_id, 'DESCONECTADO')
            broadcast_status_change(producer, cp_id, 'DESCONECTADO')

# --- 4. Lógica del Dashboard (TUI) ---
def generate_dashboard(current_telemetry_snapshot):
    """Genera la tabla de Rich para el dashboard."""
    table = Table(title=f"Panel de Control EVCharging (Actualizado: {time.strftime('%H:%M:%S')})")
    
    table.add_column("CP ID", style="cyan", no_wrap=True)
    table.add_column("Ubicación", style="magenta")
    table.add_column("Estado", style="white")
    table.add_column("Info (Precio / Suministro)", style="yellow") # Columna 4 (Híbrida)

    style_suministrando_chillon = Style(color="bright_green", bold=True)
    style_suministrando_oscuro = Style(color="green")

    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT cp_id, location, price_kwh, status FROM ChargingPoints ORDER BY cp_id")
        rows = cursor.fetchall()
        
        for row in rows:
            status = row['status']
            cp_id = row['cp_id']
            price_kwh = row['price_kwh']
            
            style = None
            info_text = ""

            if status == 'SUMINISTRANDO':
                if int(time.time()) % 2 == 0:
                    style = style_suministrando_chillon
                else:
                    style = style_suministrando_oscuro
                
                telemetry = current_telemetry_snapshot.get(cp_id)
                if telemetry:
                    info_text = f"{telemetry['kwh']:.2f} kWh | {telemetry['euros']:.2f} €"
                else:
                    info_text = "Iniciando..."
            
            else:
                style = STATUS_COLORS.get(status, Style(color="white"))
                info_text = f"{price_kwh:.2f} €/kWh"
            
            table.add_row(
                row['cp_id'],
                row['location'],
                f"{status}",
                info_text,
                style=style
            )
            
    except sqlite3.Error as e:
        print(f"[TUI_ERROR] No se pudo leer la BBDD: {e}")
    finally:
        conn.close()
        
    return Panel(table)

if __name__ == "__main__":
    # --- Globales para comandos de Central ---
    active_socket_connections = {}
    connections_lock = threading.Lock()
    
    # --- Globales de Telemetría ---
    current_telemetry = {}
    telemetry_lock = threading.Lock()
    
    # 0. Inicializar la BBDD
    if not os.path.exists(DB_NAME):
        try:
            import init_db
            init_db.create_tables()
        except ImportError:
            print(" Error: No se encuentra 'init_db.py'.")
            sys.exit(1)
        except Exception as e:
            print(f" Error inicializando la BBDD: {e}")
            sys.exit(1)

    # 1. Inicializar el Productor de Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f" Error conectando con Kafka en {KAFKA_SERVER}: {e}")
        sys.exit(1)

    # 2. Iniciar hilo del Servidor de Sockets
    socket_thread = threading.Thread(
        target=start_socket_server, 
        args=(producer, active_socket_connections, connections_lock),
        daemon=True
    )
    socket_thread.start()

    # 3. Iniciar hilo del Consumidor de Kafka
    kafka_thread = threading.Thread(
        target=start_kafka_listener, 
        args=(producer, telemetry_lock, current_telemetry), # Pasar los nuevos globales
        daemon=True
    )
    kafka_thread.start()

    # 4. Iniciar hilo del Vigilante de Heartbeats
    heartbeat_thread = threading.Thread(target=check_cp_heartbeats, args=(producer,), daemon=True)
    heartbeat_thread.start()
    
    # 5. Iniciar hilo de Comandos de Admin
    admin_thread = threading.Thread(
        target=central_command_input, 
        args=(producer, active_socket_connections, connections_lock),
        daemon=True
    )
    admin_thread.start()
    
    # 6. Iniciar el Dashboard TUI en el hilo principal
    print("Iniciando Dashboard... (Presiona Ctrl+C para salir)")
    time.sleep(1)

    # Bucle Live con screen=False para que funcione el input()
    with Live(generate_dashboard({}), refresh_per_second=1, screen=False) as live:
        try:
            while True:
                time.sleep(1)
                
                # Crear un snapshot thread-safe de la telemetría
                telemetry_snapshot = {}
                with telemetry_lock:
                    telemetry_snapshot = current_telemetry.copy()
                
                # Pasar el snapshot a la función de renderizado
                live.update(generate_dashboard(telemetry_snapshot))
                
        except KeyboardInterrupt:
            print("\n Deteniendo EV_Central...")
        finally:
            producer.close()
            print("Productor de Kafka cerrado. Adiós.")