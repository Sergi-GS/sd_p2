# EV_Central.py
import sys
import time
import queue
import json
import threading
import sqlite3
import socket
import os
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# --- Lógica de Path para importar la GUI ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(PARENT_DIR)

# Importamos la GUI de Tkinter
from central_gui import CentralApp  

# --- Constantes del Backend ---
DB_NAME = 'ev_central.db'
SOCKET_HOST = '0.0.0.0'
SOCKET_PORT = 8000
KAFKA_SERVER = 'localhost:9092'
HEARTBEAT_TIMEOUT = 15

# --- Funciones de BBDD (Lógica de Sergi) ---
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
    (Lógica de Sergi)
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO ChargingPoints (cp_id, location, price_kwh, status, last_heartbeat, last_update)
            VALUES (?, ?, ?, 'DESCONECTADO', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(cp_id) DO UPDATE SET
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

def get_cp_info_from_db(cp_id):
    """Obtiene el estado y el precio de un CP. (Lógica de Sergi)"""
    conn = None
    info = {'status': None, 'price_kwh': 0.50} # Precio por defecto
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

# --- ¡NUEVA FUNCIÓN AÑADIDA! ---
def get_charge_history_for_driver(driver_id):
    """Consulta la BBDD y devuelve el historial de recargas de un driver."""
    logs = []
    conn = None
    try:
        conn = get_db_connection()
        # Busca todas las recargas de ese driver
        cursor = conn.execute(
            "SELECT * FROM ChargeLog WHERE driver_id = ? ORDER BY start_time DESC LIMIT 10", 
            (driver_id,)
        )
        for row in cursor.fetchall():
            # Convierte la fila de BBDD en un diccionario simple
            logs.append(dict(row))
            
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al buscar historial para {driver_id}: {e}")
    finally:
        if conn:
            conn.close()
    return logs
# --- FIN DE LA NUEVA FUNCIÓN ---

# --- Funciones de Kafka/Socket (Lógica de Sergi) ---

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

def handle_socket_client(conn, addr, producer, active_connections, lock, gui_queue):
    """
    Gestiona un cliente (Monitor) de socket.
    ¡Ahora también gestiona peticiones de historial de Drivers!
    """
    print(f"[SOCKET] Nueva conexión desde: {addr}")
    cp_id_autenticado = None
    try:
        # Leemos el primer mensaje para saber quién es
        data = conn.recv(1024).decode('utf-8')
        if not data:
            conn.close()
            return
            
        messages = data.strip().split('\n')
        msg = messages[0] # Solo procesamos el primer mensaje
        parts = msg.strip().split(';')
        command = parts[0]

        # --- ¡NUEVO BLOQUE DE LÓGICA! ---
        # --- OPCIÓN 1: Es un DRIVER pidiendo su historial ---
        if command == 'GET_HISTORY':
            driver_id = parts[1]
            print(f"Driver '{driver_id}' ha solicitado su historial.")
            
            # 1. Buscar en la BBDD
            history_logs = get_charge_history_for_driver(driver_id)
            
            # 2. Convertir a JSON y enviar de vuelta
            response_json = json.dumps(history_logs)
            conn.sendall(response_json.encode('utf-8'))
            
            print(f"Historial enviado a '{driver_id}'. Cerrando conexión de driver.")
            # 3. Cerrar la conexión (fue una petición única)
            conn.close()
            return # Termina el hilo para este driver
        # --- FIN DEL NUEVO BLOQUE ---

        # --- OPCIÓN 2: Es un MONITOR registrándose ---
        elif command == 'REGISTER':
            cp_id_autenticado = parts[1]
            location = parts[2]
            price = float(parts[3])
            register_cp_in_db(cp_id_autenticado, location, price)
            update_cp_status_in_db(cp_id_autenticado, "DESCONECTADO") 
            
            print(f"CP '{cp_id_autenticado}' REGISTRADO.")
            conn.send(b"ACK;REGISTER_OK\n")
            
            with lock:
                active_connections[cp_id_autenticado] = conn
            
            gui_queue.put(("ADD_MESSAGE", f"CP '{cp_id_autenticado}' REGISTRADO (aún desconectado)."))

        # Si no es GET_HISTORY ni REGISTER, es un error de protocolo
        else:
            print(f"Protocolo desconocido en primer mensaje de {addr}. Comando: {command}")
            conn.close()
            return

        # --- BUCLE DE MONITOR (Solo si fue un REGISTER) ---
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
                
                if command == 'HEARTBEAT' and cp_id_autenticado:
                    update_cp_heartbeat(cp_id_autenticado)
                    conn.send(b"ACK;HEARTBEAT_OK\n")
                
                elif command == 'STATUS' and cp_id_autenticado:
                    new_status = parts[1]
                    update_cp_status_in_db(cp_id_autenticado, new_status)
                    if new_status == 'ACTIVADO':
                        update_cp_heartbeat(cp_id_autenticado)
                    broadcast_status_change(producer, cp_id_autenticado, new_status)
                    print(f"📡 Estado de '{cp_id_autenticado}' actualizado a {new_status} por Monitor.")
                    conn.send(b"ACK;STATUS_UPDATED\n")
                    
                    gui_queue.put(("ADD_MESSAGE", f"CP '{cp_id_autenticado}' reporta estado: {new_status}"))
                    gui_queue.put(("UPDATE_CP", cp_id_autenticado, new_status, None))
                
    except (ConnectionResetError, BrokenPipeError):
        if cp_id_autenticado:
            print(f"[SOCKET] Monitor {cp_id_autenticado} desconectado abruptamente.")
        else:
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
            
            gui_queue.put(("ADD_MESSAGE", f"CP '{cp_id_autenticado}' DESCONECTADO (Socket cerrado)"))
            gui_queue.put(("UPDATE_CP", cp_id_autenticado, "DESCONECTADO", None))
        
        if conn:
            conn.close()

def start_socket_server(producer, active_connections, lock, gui_queue):
    """Inicia el servidor de Sockets (pasa la gui_queue a los hilos)."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SOCKET_HOST, SOCKET_PORT))
    server.listen(5)
    print(f"OK [CONTROL] Servidor de Sockets escuchando en {SOCKET_PORT}...")
    gui_queue.put(("ADD_MESSAGE", f"Servidor Sockets OK en puerto {SOCKET_PORT}"))

    while True:
        conn, addr = server.accept()
        client_thread = threading.Thread(
            target=handle_socket_client, 
            args=(conn, addr, producer, active_connections, lock, gui_queue), # Pasa la cola
            daemon=True
        )
        client_thread.start()

# --- Lógica de Kafka (MODIFICADA para GUI y lógica de Sergi) ---
def start_kafka_listener(producer, gui_queue):
    """Inicia el consumidor/productor de Kafka (envía actualizaciones a la gui_queue)."""
    consumer = KafkaConsumer(
        'topic_requests',
        'topic_data_streaming',
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    print(f"OK [DATOS] Oyente de Kafka conectado a {KAFKA_SERVER}...")
    gui_queue.put(("ADD_MESSAGE", f"Oyente Kafka OK en {KAFKA_SERVER}"))

    for msg in consumer:
        try:
            data = msg.value
            
            if msg.topic == 'topic_requests':
                cp_id = data['cp_id']
                driver_id = data['driver_id']
                response_topic = data.get('response_topic')
                
                print(f"Petición de {driver_id} para {cp_id}")

                # --- ACTUALIZACIÓN GUI (Petición) ---
                now = datetime.now()
                gui_queue.put(("ADD_REQUEST", now.strftime("%d/%m/%y"), now.strftime("%H:%M:%S"), driver_id, cp_id))

                # --- Lógica de Sergi (Obtener precio) ---
                cp_info = get_cp_info_from_db(cp_id)
                status = cp_info['status']
                price_kwh = cp_info['price_kwh']
                
                if status == 'ACTIVADO':
                    update_cp_status_in_db(cp_id, 'SUMINISTRANDO')
                    print(f"OK Petición APROBADA. Enviando orden a {cp_id} al precio de {price_kwh} €/kWh...")
                    
                    producer.send(
                        f'topic_commands_{cp_id}',
                        {
                            'action': 'START_CHARGE', 
                            'driver_id': driver_id,
                            'price_kwh': price_kwh # <-- Lógica de Sergi
                        }
                    )
                    broadcast_status_change(producer, cp_id, 'SUMINISTRANDO')
                    
                    if response_topic:
                        producer.send(
                            response_topic,
                            {'status': 'APPROVED', 'cp_id': cp_id, 'driver_id': driver_id}
                        )
                    
                    # --- ACTUALIZACIÓN GUI (Aprobada) ---
                    gui_data = {"driver": driver_id, "kwh": "0.0", "eur": "0.00"}
                    gui_queue.put(("UPDATE_CP", cp_id, "SUMINISTRANDO", gui_data))
                    gui_queue.put(("ADD_MESSAGE", f"Petición de {driver_id} para {cp_id} APROBADA."))

                else:
                    print(f"Petición DENEGADA. {cp_id} no está 'ACTIVADO' (estado: {status}).")
                    if response_topic:
                        try:
                            producer.send(
                                response_topic,
                                {'status': 'DENIED', 'cp_id': cp_id, 'driver_id': driver_id, 'reason': f"CP no disponible. Estado actual: {status}"}
                            )
                            producer.flush()
                        except Exception as e:
                            print(f"[KAFKA_ERROR] No se pudo enviar 'DENIED' a {response_topic}: {e}")
                    
                    # --- ACTUALIZACIÓN GUI (Denegada) ---
                    gui_queue.put(("ADD_MESSAGE", f"Petición de {driver_id} para {cp_id} DENEGADA (Estado: {status})"))

            elif msg.topic == 'topic_data_streaming':
                charge_status = data.get('status')
                cp_id = data.get('cp_id')

                if charge_status == 'SUMINISTRANDO':
                    # --- ACTUALIZACIÓN GUI (Streaming en tiempo real) ---
                    gui_data = {
                        "driver": data.get('driver_id'),
                        "kwh": f"{data.get('kwh', 0.0):.1f}",
                        "eur": f"{data.get('euros', 0.0):.2f}"
                    }
                    gui_queue.put(("UPDATE_CP", cp_id, "SUMINISTRANDO", gui_data))
                
                # --- Lógica de Sergi (FINALIZADO, AVERIA, PARADA) ---
                elif charge_status in ('FINALIZADO', 'FINALIZADO_AVERIA', 'FINALIZADO_PARADA'):
                    # 1. Guardar log
                    try:
                        conn = get_db_connection()
                        conn.execute(
                            "INSERT INTO ChargeLog (cp_id, driver_id, start_time, end_time, total_kwh, total_euros) VALUES (?, ?, ?, ?, ?, ?)",
                            (cp_id, data.get('driver_id'), data.get('start_time'), data.get('end_time'), data.get('total_kwh'), data.get('total_euros'))
                        )
                        conn.commit()
                        print(f"💾 Log de recarga guardado para {cp_id}.")
                    except sqlite3.Error as e:
                        print(f"[DB_ERROR] No se pudo guardar el ChargeLog: {e}")
                    finally:
                        if conn:
                            conn.close()

                    # 2. Actualizar estado y GUI
                    if charge_status == 'FINALIZADO':
                        print(f"✅ Carga finalizada en {cp_id}. Volviendo a ACTIVADO.")
                        update_cp_status_in_db(cp_id, 'ACTIVADO')
                        broadcast_status_change(producer, cp_id, 'ACTIVADO')
                        gui_queue.put(("UPDATE_CP", cp_id, "ACTIVADO", None))
                        gui_queue.put(("ADD_MESSAGE", f"Carga en {cp_id} FINALIZADA."))
                    
                    elif charge_status == 'FINALIZADO_AVERIA':
                        print(f"❌ Carga interrumpida por avería en {cp_id}. El CP permanece AVERIADO.")
                        gui_queue.put(("UPDATE_CP", cp_id, "AVERIADO", None))
                        gui_queue.put(("ADD_MESSAGE", f"Carga en {cp_id} INTERRUMPIDA por avería."))
                    
                    elif charge_status == 'FINALIZADO_PARADA':
                        print(f"🛑 Carga interrumpida por parada admin en {cp_id}. El CP permanece PARADO.")
                        gui_queue.put(("UPDATE_CP", cp_id, "PARADO", None))
                        gui_queue.put(("ADD_MESSAGE", f"Carga en {cp_id} PARADA por admin."))

        except json.JSONDecodeError:
            print(f"[KAFKA_ERROR] Mensaje malformado: {msg.value}")
        except Exception as e:
            print(f"[KAFKA_ERROR] Error procesando mensaje: {e}")

# --- Lógica de Resiliencia (MODIFICADA para GUI) ---
def check_cp_heartbeats(producer, gui_queue):
    """Hilo de vigilancia que comprueba los 'last_heartbeat'."""
    print(f"✅ [RESILIENCIA] Vigilante de heartbeats iniciado (Timeout: {HEARTBEAT_TIMEOUT}s)...")
    gui_queue.put(("ADD_MESSAGE", f"Vigilante Heartbeats OK (Timeout {HEARTBEAT_TIMEOUT}s)"))
    
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
            print(f"[RESILIENCIA] No se recibió heartbeat de {cp_id}. Marcando como DESCONECTADO.")
            
            update_cp_status_in_db(cp_id, 'DESCONECTADO')
            broadcast_status_change(producer, cp_id, 'DESCONECTADO')
            
            # --- ACTUALIZACIÓN GUI ---
            gui_queue.put(("UPDATE_CP", cp_id, "DESCONECTADO", None))
            gui_queue.put(("ADD_MESSAGE", f"[RESILIENCIA] Heartbeat perdido para {cp_id} -> DESCONECTADO"))


# --- NUEVA Lógica para Carga Inicial de la GUI ---
def get_initial_cps_from_db():
    """
    Consulta la BBDD al arrancar para poblar la GUI.
    Calcula las posiciones de la rejilla (grid) dinámicamente.
    """
    print("Cargando CPs iniciales desde la BBDD para la GUI...")
    cps_para_gui = []
    conn = None
    try:
        conn = get_db_connection()
        # Usamos la BBDD de Sergi (ej. populate_db.py)
        cursor = conn.execute("SELECT cp_id, location, price_kwh FROM ChargingPoints ORDER BY cp_id")
        rows = cursor.fetchall()
        
        for i, row in enumerate(rows):
            cps_para_gui.append({
                "id": row['cp_id'],
                "loc": row['location'],
                "price": f"{row['price_kwh']:.2f}€/kWh",
                "grid_row": i // 5,  # 5 columnas por fila
                "grid_col": i % 5
            })
        print(f"Cargados {len(rows)} CPs en la rejilla de la GUI.")
    except sqlite3.Error as e:
        print(f"[GUI_ERROR] No se pudo leer la BBDD para la carga inicial: {e}")
    finally:
        if conn:
            conn.close()
    
    return cps_para_gui

# --- NUEVA Clase Conectora (para los botones de la GUI) ---
class BackendConnector:
    """
    Esta clase actúa como "controlador" para la GUI.
    La GUI llama a estos métodos cuando se pulsa un botón.
    (Lógica de Sergi)
    """
    def __init__(self, producer, active_connections, lock, gui_queue):
        self.producer = producer
        self.active_connections = active_connections
        self.lock = lock
        self.gui_queue = gui_queue

    def request_parar_cp(self, cp_id):
        """Lógica para el botón 'Parar CP'."""
        print(f"ADMIN: Petición de PARAR {cp_id}")
        self._send_admin_command(cp_id, 'PARADO', 'STOP_CP')

    def request_reanudar_cp(self, cp_id):
        """Lógica para el botón 'Reanudar CP'."""
        print(f"ADMIN: Petición de REANUDAR {cp_id}")
        self._send_admin_command(cp_id, 'ACTIVADO', 'RESUME_CP')

    def _send_admin_command(self, cp_id, new_status, socket_cmd):
        target_conn = None
        with self.lock:
            target_conn = self.active_connections.get(cp_id)
        
        if target_conn:
            try:
                # 1. Envía el comando al Monitor por el socket
                send_socket_message(target_conn, socket_cmd)
                
                # 2. Actualiza la BBDD
                update_cp_status_in_db(cp_id, new_status)
                
                # 3. Difunde el cambio por Kafka
                broadcast_status_change(self.producer, cp_id, new_status)
                
                # 4. Actualiza la GUI
                self.gui_queue.put(("ADD_MESSAGE", f"Comando '{new_status}' enviado a {cp_id}."))
                self.gui_queue.put(("UPDATE_CP", cp_id, new_status, None))
                
            except Exception as e:
                print(f"❌ [ADMIN] Error enviando comando a {cp_id}: {e}")
                self.gui_queue.put(("ADD_MESSAGE", f"ERROR enviando comando a {cp_id}"))
        else:
            print(f"❌ [ADMIN] {cp_id} no está conectado (socket no encontrado).")
            self.gui_queue.put(("ADD_MESSAGE", f"ERROR: {cp_id} no está conectado (socket no encontrado)."))


# --- Hilo Principal (MODIFICADO para GUI) ---

if __name__ == "__main__":
    # --- Globales para hilos de backend ---
    active_socket_connections = {} 
    connections_lock = threading.Lock()
    
    # 0. Crear la cola de comunicación
    gui_queue = queue.Queue()

    # 1. Inicializar la BBDD
    if not os.path.exists(DB_NAME):
        try:
            import init_db
            init_db.create_tables()
            gui_queue.put(("ADD_MESSAGE", "BBDD no encontrada. Tablas creadas."))
            
            # Si creamos la BBDD, poblamos datos de ejemplo
            import populate_db
            populate_db.populate_data()
            gui_queue.put(("ADD_MESSAGE", "BBDD poblada con datos de ejemplo."))
            
        except ImportError:
            print("Error: No se encuentra 'init_db.py' o 'populate_db.py'.")
            sys.exit(1)
        except Exception as e:
            print(f"Error inicializando la BBDD: {e}")
            sys.exit(1)

    # 2. Inicializar el Productor de Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        gui_queue.put(("ADD_MESSAGE", "Productor Kafka OK."))
    except Exception as e:
        print(f"Error conectando con Kafka en {KAFKA_SERVER}: {e}")
        gui_queue.put(("ADD_MESSAGE", f"ERROR conectando a Kafka: {e}"))
        sys.exit(1)

    # --- Configuración de la GUI ---
    
    # 3. Iniciar la aplicación GUI (en el hilo principal)
    app = CentralApp(gui_queue)
    
    # 4. Crear el conector del backend y pasárselo a la GUI
    backend_connector = BackendConnector(producer, active_socket_connections, connections_lock, gui_queue)
    app.set_controller(backend_connector)

    # 5. Cargar los CPs iniciales desde la BBDD a la GUI
    initial_cps = get_initial_cps_from_db()
    app.load_initial_cps(initial_cps)

    # --- Iniciar Hilos de Backend ---

    # 6. Iniciar hilo del Servidor de Sockets
    socket_thread = threading.Thread(
        target=start_socket_server, 
        args=(producer, active_socket_connections, connections_lock, gui_queue), 
        daemon=True
    )
    socket_thread.start()

    # 7. Iniciar hilo del Consumidor de Kafka
    kafka_thread = threading.Thread(
        target=start_kafka_listener, 
        args=(producer, gui_queue),
        daemon=True
    )
    kafka_thread.start()

    # 8. Iniciar hilo del Vigilante de Heartbeats
    heartbeat_thread = threading.Thread(
        target=check_cp_heartbeats, 
        args=(producer, gui_queue),
        daemon=True
    )
    heartbeat_thread.start()
    
    # 9. Iniciar el bucle principal de la GUI
    print("Iniciando GUI... (EV_Central en funcionamiento)")
    gui_queue.put(("ADD_MESSAGE", "CENTRAL system status OK"))
    
    try:
        app.mainloop()
    except KeyboardInterrupt:
        print("\nCerrando EV_Central...")
    finally:
        producer.close()
        print("Productor de Kafka cerrado. Adiós.")