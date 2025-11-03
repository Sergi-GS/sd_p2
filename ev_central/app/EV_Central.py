# EV_Central.py
import sys
import time
import json
import threading
import sqlite3
import socket
import os  # Importado para la inicialización de la BBDD
from kafka import KafkaConsumer, KafkaProducer
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.style import Style

# --- Constantes ---
DB_NAME = 'ev_central.db'  # La BBDD debe estar en la misma carpeta
SOCKET_HOST = '0.0.0.0'
SOCKET_PORT = 8000
# KAFKA_SERVER se ajusta para el desarrollo local (conectando a Docker)
KAFKA_SERVER = 'localhost:9092' 
HEARTBEAT_TIMEOUT = 15  # Segundos para marcar un CP como DESCONECTADO

# Colores para el dashboard TUI
STATUS_COLORS = {
    'ACTIVADO': Style(color="green"),
    'SUMINISTRANDO': Style(color="green", bold=True),
    'AVERIADO': Style(color="red"),
    'PARADO': Style(color="yellow"),
    'DESCONECTADO': Style(color="grey50"),
}

# --- Lógica de Base de Datos ---

def get_db_connection():
    """Obtiene una conexión a la BBDD."""
    # check_same_thread=False es crucial para que múltiples hilos usen SQLite
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # Para acceder a los datos por nombre de columna
    # Habilitar 'Write-Ahead Logging' para mejor concurrencia
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def update_cp_status_in_db(cp_id, new_status, conn=None):
    """Actualiza el estado de un CP en la BBDD."""
    local_conn = False
    if conn is None:
        conn = get_db_connection()
        local_conn = True
    
    try:
        conn.execute(
            "UPDATE ChargingPoints SET status = ?, last_update = CURRENT_TIMESTAMP WHERE cp_id = ?",
            (new_status, cp_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al actualizar estado: {e}")
    finally:
        if local_conn:
            conn.close()

def register_cp_in_db(cp_id, location, price, conn=None):
    """Registra o actualiza un CP en la BBDD."""
    local_conn = False
    if conn is None:
        conn = get_db_connection()
        local_conn = True

    try:
        # Inserta o actualiza el CP. Asume que el estado inicial es DESCONECTADO
        # El socket handler lo pondrá 'ACTIVADO' inmediatamente
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
        if local_conn:
            conn.close()

def update_cp_heartbeat(cp_id, conn=None):
    """Actualiza la marca de tiempo 'last_heartbeat' de un CP."""
    local_conn = False
    if conn is None:
        conn = get_db_connection()
        local_conn = True
        
    try:
        conn.execute(
            "UPDATE ChargingPoints SET last_heartbeat = CURRENT_TIMESTAMP WHERE cp_id = ?",
            (cp_id,)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al actualizar heartbeat: {e}")
    finally:
        if local_conn:
            conn.close()

def get_cp_status_from_db(cp_id, conn=None):
    """Obtiene el estado actual de un CP."""
    local_conn = False
    if conn is None:
        conn = get_db_connection()
        local_conn = True
        
    status = None
    try:
        cursor = conn.execute("SELECT status FROM ChargingPoints WHERE cp_id = ?", (cp_id,))
        result = cursor.fetchone()
        if result:
            status = result['status']
    except sqlite3.Error as e:
        print(f"[DB_ERROR] al leer estado: {e}")
    finally:
        if local_conn:
            conn.close()
    return status

def broadcast_status_change(producer, cp_id, new_status, location=None, price=None):
    """Envía el estado actualizado al topic de broadcast."""
    payload = {
        'cp_id': cp_id,
        'status': new_status,
    }
    # Incluir detalles adicionales si están disponibles (útil en el registro)
    if location:
        payload['location'] = location
    if price:
        payload['price_kwh'] = price
        
    producer.send('topic_status_broadcast', payload)
    producer.flush()

# --- 1. Lógica del Servidor de Sockets (Canal de Control) ---

def handle_socket_client(conn, addr, producer):
    """Maneja la conexión de un único EV_CP_M (Monitor) por Sockets."""
    print(f"[SOCKET] Nueva conexión del Monitor: {addr}")
    cp_id_autenticado = None
    db_conn = get_db_connection() # Conexión única para este hilo
    try:
        while True:
            # TODO: Implementar el protocolo <STX>...<ETX><LRC>
            data = conn.recv(1024).decode('utf-8')
            if not data:
                break  # Cliente desconectado
            
            # --- Lógica de protocolo (simplificada) ---
            messages = data.strip().split('\n') # Manejar mensajes agrupados
            for msg in messages:
                if not msg:
                    continue
                
                parts = msg.strip().split(';')
                command = parts[0]
                
                if command == 'REGISTER':
                    cp_id_autenticado = parts[1]
                    location = parts[2]
                    price = float(parts[3])
                    register_cp_in_db(cp_id_autenticado, location, price, db_conn)
                    update_cp_status_in_db(cp_id_autenticado, "ACTIVADO", db_conn)
                    broadcast_status_change(producer, cp_id_autenticado, "ACTIVADO", location, price)
                    print(f"✅  CP '{cp_id_autenticado}' REGISTRADO y ACTIVADO.")
                    conn.send(b"ACK;REGISTER_OK\n")
                
                elif command == 'HEARTBEAT' and cp_id_autenticado:
                    update_cp_heartbeat(cp_id_autenticado, db_conn)
                    conn.send(b"ACK;HEARTBEAT_OK\n")
                
                elif command == 'STATUS' and cp_id_autenticado:
                    new_status = parts[1]  # Ej: "AVERIADO" o "ACTIVADO" (recuperado)
                    update_cp_status_in_db(cp_id_autenticado, new_status, db_conn)
                    broadcast_status_change(producer, cp_id_autenticado, new_status)
                    print(f"📡  Estado de '{cp_id_autenticado}' actualizado a {new_status} por Monitor.")
                    conn.send(b"ACK;STATUS_UPDATED\n")
                
                # TODO: Añadir lógica para recibir comandos de CENTRAL (Parar/Reanudar)
                # (Esta parte es más compleja, Central necesita "empujar" un msg a este hilo)

    except (ConnectionResetError, BrokenPipeError):
        print(f"[SOCKET] Cliente {addr} desconectado abruptamente.")
    except Exception as e:
        print(f"[SOCKET] Error con {addr}: {e}")
    finally:
        if cp_id_autenticado:
            print(f"🔌 [SOCKET] Conexión cerrada. '{cp_id_autenticado}' pasa a DESCONECTADO.")
            update_cp_status_in_db(cp_id_autenticado, "DESCONECTADO", db_conn)
            broadcast_status_change(producer, cp_id_autenticado, "DESCONECTADO")
        db_conn.close()
        conn.close()

def start_socket_server(producer):
    """Inicia el servidor de Sockets concurrente para los Monitores."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SOCKET_HOST, SOCKET_PORT))
    server.listen(5)
    print(f"✅ [CONTROL] Servidor de Sockets escuchando en {SOCKET_PORT}...")

    while True:
        conn, addr = server.accept()
        # Pasa el productor de Kafka al hilo del cliente
        client_thread = threading.Thread(target=handle_socket_client, args=(conn, addr, producer), daemon=True)
        client_thread.start()

# --- 2. Lógica de Kafka (Canal de Datos) ---

def start_kafka_listener(producer):
    """Inicia el consumidor/productor de Kafka para los Drivers y Engines."""
    consumer = KafkaConsumer(
        'topic_requests',        # Peticiones de Drivers
        'topic_data_streaming',  # Datos de Engines (para saber cuándo finaliza)
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    print(f"✅ [DATOS] Oyente de Kafka conectado a {KAFKA_SERVER}...")

    for msg in consumer:
        try:
            data = msg.value
            
            if msg.topic == 'topic_requests':
                cp_id = data['cp_id']
                driver_id = data['driver_id']
                print(f"🚗  Petición de {driver_id} para {cp_id}")

                # 1. Comprueba BBDD (¿está "ACTIVADO"?)
                status = get_cp_status_from_db(cp_id)
                
                if status == 'ACTIVADO':
                    # 2. Si OK, actualiza BBDD a "SUMINISTRANDO"
                    update_cp_status_in_db(cp_id, 'SUMINISTRANDO')
                    print(f"👍  Petición APROBADA. Enviando orden a {cp_id}...")
                    
                    # 3. Envía msg a 'topic_commands_[CP_ID]' para que el ENGINE empiece
                    producer.send(
                        f'topic_commands_{cp_id}',
                        {'action': 'START_CHARGE', 'driver_id': driver_id}
                    )
                    # 4. Difunde el cambio de estado
                    broadcast_status_change(producer, cp_id, 'SUMINISTRANDO')
                
                else:
                    # TODO: Enviar respuesta de "denegado" al driver
                    print(f"👎  Petición DENEGADA. {cp_id} no está 'ACTIVADO' (estado: {status}).")

            elif msg.topic == 'topic_data_streaming':
                # Escuchamos este topic solo para saber cuándo termina una carga
                if data.get('status') == 'FINALIZADO':
                    cp_id = data['cp_id']
                    print(f"✅  Carga finalizada en {cp_id}. Volviendo a ACTIVADO.")
                    
                    # 1. Actualiza estado del CP en BBDD a "ACTIVADO"
                    update_cp_status_in_db(cp_id, 'ACTIVADO')
                    
                    # 2. (Opcional) Guarda el log final en BBDD
                    # ... (INSERT INTO ChargeLog ...)
                    
                    # 3. Difunde el cambio de estado
                    broadcast_status_change(producer, cp_id, 'ACTIVADO')
                    
        except json.JSONDecodeError:
            print(f"[KAFKA_ERROR] Mensaje malformado: {msg.value}")
        except Exception as e:
            print(f"[KAFKA_ERROR] Error procesando mensaje: {e}")

# --- 3. Lógica de Resiliencia ---

def check_cp_heartbeats(producer):
    """Hilo de vigilancia que comprueba los 'last_heartbeat'."""
    print(f"✅ [RESILIENCIA] Vigilante de heartbeats iniciado (Timeout: {HEARTBEAT_TIMEOUT}s)...")
    while True:
        time.sleep(HEARTBEAT_TIMEOUT // 2)
        
        conn = get_db_connection()
        try:
            # 1. Buscar CPs donde (now - last_heartbeat) > TIMEOUT
            #    y que no estén ya marcados como DESCONECTADO
            cursor = conn.execute(
                """
                SELECT cp_id, status FROM ChargingPoints
                WHERE status != 'DESCONECTADO' 
                AND (STRFTIME('%s', 'now') - STRFTIME('%s', last_heartbeat)) > ?
                """,
                (HEARTBEAT_TIMEOUT,)
            )
            stale_cps = cursor.fetchall()
            
            for row in stale_cps:
                cp_id = row['cp_id']
                print(f"💔 [RESILIENCIA] No se recibió heartbeat de {cp_id}. Marcando como DESCONECTADO.")
                
                # 2. Para cada uno, llamar a update_cp_status_in_db
                update_cp_status_in_db(cp_id, 'DESCONECTADO', conn)
                
                # 3. Difundir el cambio de estado
                broadcast_status_change(producer, cp_id, 'DESCONECTADO')
                
        except sqlite3.Error as e:
            print(f"[RESILIENCIA_ERROR] Error en BBDD: {e}")
        finally:
            conn.close()

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
    # 0. Inicializar la BBDD (llamar a init_db.py si no existe)
    if not os.path.exists(DB_NAME):
        try:
            import init_db
            init_db.create_tables()
            # Asumimos que init_db.py tiene una columna 'last_heartbeat' DATETIME
            # y 'last_update' DATETIME en la tabla ChargingPoints
        except ImportError:
            print("❌ Error: No se encuentra 'init_db.py'.")
            print("Por favor, crea la BBDD manualmente o asegúrate de que el script está presente.")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error inicializando la BBDD: {e}")
            sys.exit(1)

    # 1. Inicializar el Productor de Kafka (compartido por todos los hilos)
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Error fatal conectando con Kafka en {KAFKA_SERVER}: {e}")
        print("Asegúrate de que Kafka está corriendo en Docker.")
        sys.exit(1)

    # 2. Iniciar hilo del Servidor de Sockets
    socket_thread = threading.Thread(target=start_socket_server, args=(producer,), daemon=True)
    socket_thread.start()

    # 3. Iniciar hilo del Consumidor de Kafka
    kafka_thread = threading.Thread(target=start_kafka_listener, args=(producer,), daemon=True)
    kafka_thread.start()

    # 4. Iniciar hilo del Vigilante de Heartbeats
    heartbeat_thread = threading.Thread(target=check_cp_heartbeats, args=(producer,), daemon=True)
    heartbeat_thread.start()
    
    # 5. Iniciar el Dashboard TUI en el hilo principal
    print("Iniciando Dashboard... (Presiona Ctrl+C para salir)")
    time.sleep(1)  # Dar tiempo a que los hilos arranquen

    with Live(generate_dashboard(), refresh_per_second=1, screen=True) as live:
        try:
            while True:
                time.sleep(1)
                live.update(generate_dashboard())
        except KeyboardInterrupt:
            print("\n🛑 Deteniendo EV_Central...")
        finally:
            producer.close()
            print("Productor de Kafka cerrado. Adiós.")