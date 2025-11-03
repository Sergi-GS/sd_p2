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
    'SUMINISTRANDO': Style(color="green", bold=True),
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

# EV_Central.py (SOLO REEMPLAZA ESTAS 4 FUNCIONES)

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
    """Registra o actualiza un CP en la BBDD."""
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO ChargingPoints (cp_id, location, price_kwh, status, last_heartbeat, last_update)
            VALUES (?, ?, ?, 'DESCONECTADO', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(cp_id) DO UPDATE SET
                location = excluded.location,
                price_kwh = excluded.price_kwh,
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

def get_cp_status_from_db(cp_id):
    """Obtiene el estado actual de un CP."""
    conn = None
    status = None
    try:
        conn = get_db_connection()
        cursor = conn.execute("SELECT status FROM ChargingPoints WHERE cp_id = ?", (cp_id,))
        result = cursor.fetchone()
        if result:
            status = result['status']
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al leer estado: {e}")
    finally:
        if conn:
            conn.close()
    return status



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

### --- FUNCIÓN AÑADIDA --- ###
def send_socket_message(conn, message_str):
    """Envía un mensaje de texto simple con un salto de línea."""
    try:
        conn.sendall(f"{message_str}\n".encode('utf-8'))
    except (BrokenPipeError, ConnectionResetError):
        # El socket murió, el handle_socket_client se encargará
        print(f"[SOCKET_SEND] Error: La conexión ya estaba cerrada.")
### --- FIN FUNCIÓN AÑADIDA --- ###


#sockets
### --- MODIFICADA LA FIRMA --- ###
def handle_socket_client(conn, addr, producer, active_connections, lock):
    print(f"[SOCKET] Nueva conexión del Monitor: {addr}")
    cp_id_autenticado = None
    try:
        while True:
            # TODO: Implementar el protocolo <STX>...<ETX><LRC>
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
                    update_cp_status_in_db(cp_id_autenticado, "ACTIVADO")
                    broadcast_status_change(producer, cp_id_autenticado, "ACTIVADO", location, price)
                    print(f"CP '{cp_id_autenticado}' REGISTRADO y ACTIVADO.")
                    conn.send(b"ACK;REGISTER_OK\n")
                    
                    ### --- LÓGICA AÑADIDA --- ###
                    # Guardar la conexión en el diccionario global
                    with lock:
                        active_connections[cp_id_autenticado] = conn
                    ### --- FIN LÓGICA AÑADIDA --- ###
                
                elif command == 'HEARTBEAT' and cp_id_autenticado:
                    update_cp_heartbeat(cp_id_autenticado)
                    conn.send(b"ACK;HEARTBEAT_OK\n")
                
                elif command == 'STATUS' and cp_id_autenticado:
                    new_status = parts[1]  # Ej: "AVERIADO" o "ACTIVADO" (recuperado)
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
            
            ### --- LÓGICA AÑADIDA --- ###
            # Eliminar la conexión del diccionario global
            with lock:
                active_connections.pop(cp_id_autenticado, None)
            ### --- FIN LÓGICA AÑADIDA --- ###
            
      
        conn.close()

### --- MODIFICADA LA FIRMA --- ###
def start_socket_server(producer, active_connections, lock):
    """Inicia el servidor de Sockets concurrente para los Monitores."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SOCKET_HOST, SOCKET_PORT))
    server.listen(5)
    print(f"OK [CONTROL] Servidor de Sockets escuchando en {SOCKET_PORT}...")

    while True:
        conn, addr = server.accept()
        
        ### --- MODIFICADA LA CREACIÓN DEL HILO --- ###
        # Pasa el productor de Kafka Y los globales al hilo del cliente
        client_thread = threading.Thread(
            target=handle_socket_client, 
            args=(conn, addr, producer, active_connections, lock), 
            daemon=True
        )
        ### --- FIN MODIFICACIÓN --- ###
        client_thread.start()

#kafka
def start_kafka_listener(producer):
    """Inicia el consumidor/productor de Kafka para los Drivers y Engines."""
    consumer = KafkaConsumer(
        'topic_requests',        # Peticiones de Drivers
        'topic_data_streaming',  # Datos de Engines (para saber cuándo finaliza)
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
                # El Driver debe incluir este campo en su petición
                response_topic = data.get('response_topic') # Ej: "topic_driver_user123"
                
                print(f" Petición de {driver_id} para {cp_id}")

                # 1. Comprueba BBDD (¿está "ACTIVADO"?)
                status = get_cp_status_from_db(cp_id)
                
                if status == 'ACTIVADO':
                    # 2. Si OK, actualiza BBDD a "SUMINISTRANDO"
                    update_cp_status_in_db(cp_id, 'SUMINISTRANDO')
                    print(f"OK  Petición APROBADA. Enviando orden a {cp_id}...")
                    
                    # 3. Envía msg a 'topic_commands_[CP_ID]' para que el ENGINE empiece
                    producer.send(
                        f'topic_commands_{cp_id}',
                        {'action': 'START_CHARGE', 'driver_id': driver_id}
                    )
                    
                    # 4. Difunde el cambio de estado
                    broadcast_status_change(producer, cp_id, 'SUMINISTRANDO')
                    
                    # 5. Notificar al driver que fue APROBADO (COMPLETANDO TODO)
                    if response_topic:
                        producer.send(
                            response_topic,
                            {'status': 'APPROVED', 'cp_id': cp_id, 'driver_id': driver_id}
                        )
                
                else:
                    print(f"  Petición DENEGADA. {cp_id} no está 'ACTIVADO' (estado: {status}).")
                    
                    # --- COMPLETADO EL TODO ---
                    if response_topic:
                        try:
                            producer.send(
                                response_topic,
                                {
                                    'status': 'DENIED',
                                    'cp_id': cp_id,
                                    'driver_id': driver_id,
                                    'reason': f"CP no disponible. Estado actual: {status}"
                                }
                            )
                            producer.flush()
                        except Exception as e:
                            print(f"[KAFKA_ERROR] No se pudo enviar 'DENIED' a {response_topic}: {e}")
                    # --- FIN DEL TODO ---

            elif msg.topic == 'topic_data_streaming':
                # Escuchamos este topic solo para saber cuándo termina una carga
                if data.get('status') == 'FINALIZADO':
                    cp_id = data['cp_id']
                    print(f"✅  Carga finalizada en {cp_id}. Volviendo a ACTIVADO.")
                    
                    # 1. Actualiza estado del CP en BBDD a "ACTIVADO"
                    update_cp_status_in_db(cp_id, 'ACTIVADO')
                    
                    # --- COMPLETADO EL TODO (Opcional) ---
                    # 2. Guarda el log final en BBDD
                    try:
                        conn = get_db_connection()
                        conn.execute(
                            """
                            INSERT INTO ChargeLog (cp_id, driver_id, start_time, end_time, total_kwh, total_euros)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                cp_id,
                                data.get('driver_id'),
                                data.get('start_time'), # EV_CP_E debe enviar esto
                                data.get('end_time'),   # EV_CP_E debe enviar esto
                                data.get('total_kwh'),  # EV_CP_E debe enviar esto
                                data.get('total_euros') # EV_CP_E debe enviar esto
                            )
                        )
                        conn.commit()
                        print(f"💾 Log de recarga guardado para {cp_id}.")
                    except sqlite3.Error as e:
                        print(f"[DB_ERROR] No se pudo guardar el ChargeLog: {e}")
                    finally:
                        if conn:
                            conn.close()
                    # --- FIN DEL TODO ---
                    
                    # 3. Difunde el cambio de estado
                    broadcast_status_change(producer, cp_id, 'ACTIVADO')
                    
        except json.JSONDecodeError:
            print(f"[KAFKA_ERROR] Mensaje malformado: {msg.value}")
        except Exception as e:
            print(f"[KAFKA_ERROR] Error procesando mensaje: {e}")


def central_command_input(producer, active_connections, lock):
    """Hilo que escucha comandos del admin en la terminal de Central."""
    print("✅ [ADMIN] Hilo de comandos iniciado. Escribe 'HELP' para ver opciones.")
    while True:
        try:
            cmd_line = input("> ") # Bloqueante, por eso necesita su propio hilo
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
                        # 1. Envía el comando al Monitor por el socket
                        cmd_to_send = "STOP_CP" if command == 'STOP' else "RESUME_CP"
                        
                        ### --- MODIFICADO --- ###
                        # ¡Usamos la función de envío simple!
                        send_socket_message(target_conn, cmd_to_send)
                        ### --- FIN MODIFICACIÓN --- ###
                        
                        print(f"✅ [ADMIN] Comando '{cmd_to_send}' enviado a {cp_id}.")
                        
                        # 2. Actualiza la BBDD
                        update_cp_status_in_db(cp_id, new_status)
                        
                        # 3. Difunde el cambio por Kafka
                        broadcast_status_change(producer, cp_id, new_status)
                        
                    except Exception as e:
                        print(f"❌ [ADMIN] Error enviando comando a {cp_id}: {e}")
                else:
                    print(f"❌ [ADMIN] {cp_id} no está conectado (socket no encontrado).")

        except EOFError: # Ocurre si el input se cierra
            break
        except Exception as e:
            print(f"❌ [ADMIN] Error en hilo de comandos: {e}")

# --- 3. Lógica de Resiliencia ---

# EV_Central.py (Modifica esta función)

def check_cp_heartbeats(producer):
    """Hilo de vigilancia que comprueba los 'last_heartbeat'."""
    print(f"✅ [RESILIENCIA] Vigilante de heartbeats iniciado (Timeout: {HEARTBEAT_TIMEOUT}s)...")
    while True:
        time.sleep(HEARTBEAT_TIMEOUT // 2)
        
        # conn = get_db_connection() <-- BORRA ESTA LÍNEA
        stale_cps = [] # Lista para guardar los CP caducados
        conn_read = None # Conexión solo para leer
        try:
            # 1. Conexión para LEER
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
                conn_read.close() # Cierra la conexión de LECTURA

        # 2. Ahora, actualiza los caducados (cada uno abre su propia conexión)
        for row in stale_cps:
            cp_id = row['cp_id']
            print(f" [RESILIENCIA] No se recibió heartbeat de {cp_id}. Marcando como DESCONECTADO.")
            
            # --- LLAMADA MODIFICADA ---
            update_cp_status_in_db(cp_id, 'DESCONECTADO')
            # --- FIN MODIFICACIÓN ---
            
            broadcast_status_change(producer, cp_id, 'DESCONECTADO')
        
        # La lógica de BBDD en 'central_command_input' y 'start_kafka_listener'
        # ya usa este método (llamar a 'update_cp_status_in_db' sin 'conn'),
        # así que esas funciones ya están bien.

# --- 4. Lógica del Dashboard (TUI) ---

def generate_dashboard():
    """Genera la tabla de Rich para el dashboard."""
    table = Table(title=f"Panel de Control EVCharging (Actualizado: {time.strftime('%H:%M:%S')})")
    table.add_column("CP ID", style="cyan", no_wrap=True)
    table.add_column("Ubicación", style="magenta")
    table.add_column("Precio (€/kWh)", style="yellow")
    table.add_column("Estado", style="white")

    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT cp_id, location, price_kwh, status FROM ChargingPoints ORDER BY cp_id")
        rows = cursor.fetchall()
        
        for row in rows:
            status = row['status']
            style = STATUS_COLORS.get(status, Style(color="white"))
            table.add_row(
                row['cp_id'],
                row['location'],
                f"{row['price_kwh']:.2f}",
                f"{status}",
                style=style
            )
            
    except sqlite3.Error as e:
        print(f"[TUI_ERROR] No se pudo leer la BBDD: {e}")
    finally:
        conn.close()
        
    return Panel(table)

# --- Hilo Principal ---

if __name__ == "__main__":
    # --- Globales para comandos de Central ---
    active_socket_connections = {} # { "CP_ID": conn_object }
    connections_lock = threading.Lock()
    
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
    ### --- MODIFICADO --- ###
    socket_thread = threading.Thread(
        target=start_socket_server, 
        args=(producer, active_socket_connections, connections_lock), # Le pasamos los globales
        daemon=True
    )
    ### --- FIN MODIFICACIÓN --- ###
    socket_thread.start()

    # 3. Iniciar hilo del Consumidor de Kafka
    kafka_thread = threading.Thread(target=start_kafka_listener, args=(producer,), daemon=True)
    kafka_thread.start()

    # 4. Iniciar hilo del Vigilante de Heartbeats
    heartbeat_thread = threading.Thread(target=check_cp_heartbeats, args=(producer,), daemon=True)
    heartbeat_thread.start()
    
    ### --- HILO AÑADIDO --- ###
    # 5. Iniciar hilo de Comandos de Admin
    admin_thread = threading.Thread(
        target=central_command_input, 
        args=(producer, active_socket_connections, connections_lock), # Le pasamos los globales
        daemon=True
    )
    admin_thread.start()
    ### --- FIN HILO AÑADIDO --- ###
    
    # 6. Iniciar el Dashboard TUI en el hilo principal (renumerado)
    print("Iniciando Dashboard... (Presiona Ctrl+C para salir)")
    time.sleep(1)  # Dar tiempo a que los hilos arranquen

    with Live(generate_dashboard(), refresh_per_second=1, screen=True) as live:
        try:
            while True:
                time.sleep(1)
                live.update(generate_dashboard())
        except KeyboardInterrupt:
            print("\n Deteniendo EV_Central...")
        finally:
            producer.close()
            print("Productor de Kafka cerrado. Adiós.")