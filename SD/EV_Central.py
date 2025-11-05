# EV_Central_v2.py
import sys
import time
import json
import threading
import sqlite3
import socket
from kafka import KafkaConsumer, KafkaProducer
from rich.live import Live
from rich.table import Table
from rich.panel import Panel

# --- Constantes ---
DB_NAME = 'ev_central.db'
SOCKET_HOST = '0.0.0.0'
SOCKET_PORT = 8000
KAFKA_SERVER = 'kafka:9092' # Asumimos el nombre del servicio de Docker
HEARTBEAT_TIMEOUT = 15 # Segundos para marcar un CP como DESCONECTADO

# --- Lógica de Base de Datos ---

def get_db_connection():
    """Obtiene una conexión a la BBDD (la crea si no existe)."""
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row # Para acceder a los datos por nombre de columna
    return conn

def update_cp_status_in_db(cp_id, new_status):
    """Actualiza el estado de un CP en la BBDD."""
    # (Esta función haría el UPDATE en la tabla ChargingPoints)
    # print(f"DB_UPDATE: {cp_id} -> {new_status}")
    pass

def register_cp_in_db(cp_id, location, price):
    """Registra o actualiza un CP en la BBDD."""
    # (Esta función haría el INSERT ... ON CONFLICT ...)
    # print(f"DB_REGISTER: {cp_id} en {location}")
    pass

def update_cp_heartbeat(cp_id):
    """Actualiza la marca de tiempo 'last_heartbeat' de un CP."""
    # (Esta función haría UPDATE ... SET last_heartbeat = CURRENT_TIMESTAMP)
    pass

# --- 1. Lógica del Servidor de Sockets (Canal de Control) ---

def handle_socket_client(conn, addr):
    """Maneja la conexión de un único EV_CP_M (Monitor) por Sockets."""
    print(f"[SOCKET] Nueva conexión del Monitor: {addr}")
    cp_id_autenticado = None
    try:
        while True:
            # TODO: Implementar el protocolo <STX>...<ETX><LRC>
            data = conn.recv(1024).decode('utf-8')
            if not data:
                break # Cliente desconectado
            
            # --- Lógica de protocolo (simplificada, cambiar por STX/ETX/LRC) ---
            # Ej: "REGISTER;CP_001;C/Italia 5;0.54"
            # Ej: "HEARTBEAT;CP_001"
            # Ej: "STATUS;CP_001;AVERIADO" (El Engine falló)
            
            parts = data.strip().split(';')
            command = parts[0]
            
            if command == 'REGISTER':
                cp_id_autenticado = parts[1]
                location = parts[2]
                price = float(parts[3])
                register_cp_in_db(cp_id_autenticado, location, price)
                update_cp_status_in_db(cp_id_autenticado, "ACTIVADO")
                conn.send(b"ACK;REGISTER_OK\n") # Enviar respuesta con protocolo
            
            elif command == 'HEARTBEAT' and cp_id_autenticado:
                update_cp_heartbeat(cp_id_autenticado)
                conn.send(b"ACK;HEARTBEAT_OK\n")
            
            elif command == 'STATUS' and cp_id_autenticado:
                new_status = parts[2] # "AVERIADO" o "ACTIVADO" (recuperado)
                update_cp_status_in_db(cp_id_autenticado, new_status)
                conn.send(b"ACK;STATUS_UPDATED\n")
            
            # TODO: Añadir lógica para recibir comandos de CENTRAL (Parar/Reanudar)
            # (Esta parte es más compleja, Central necesita "empujar" un msg a este hilo)

    except Exception as e:
        print(f"[SOCKET] Error con {addr}: {e}")
    finally:
        if cp_id_autenticado:
            print(f"[SOCKET] Conexión cerrada. {cp_id_autenticado} se considera DESCONECTADO.")
            update_cp_status_in_db(cp_id_autenticado, "DESCONECTADO")
        conn.close()

def start_socket_server():
    """Inicia el servidor de Sockets concurrente para los Monitores."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((SOCKET_HOST, SOCKET_PORT))
    server.listen(5)
    print(f"✅ [CONTROL] Servidor de Sockets escuchando en {SOCKET_PORT}...")

    while True:
        conn, addr = server.accept()
        # Inicia un nuevo hilo por cada Monitor que se conecta
        client_thread = threading.Thread(target=handle_socket_client, args=(conn, addr), daemon=True)
        client_thread.start()

# --- 2. Lógica de Kafka (Canal de Datos) ---

def start_kafka_listener():
    """Inicia el consumidor/productor de Kafka para los Drivers y Engines."""
    # TODO: Inicializar Consumidor (topic_requests, topic_data_streaming)
    # TODO: Inicializar Productor (topic_commands, topic_status_broadcast)
    print(f"✅ [DATOS] Oyente de Kafka conectado a {KAFKA_SERVER}...")

    # Bucle infinito de consumo
    # for msg in consumer:
    #     if msg.topic == 'topic_requests':
    #         # 1. Recibe petición de Driver
    #         # 2. Comprueba BBDD (¿está "ACTIVADO"?)
    #         # 3. Si OK, actualiza BBDD a "SUMINISTRANDO"
    #         # 4. Envía msg a 'topic_commands_[CP_ID]' para que el ENGINE empiece
    #         pass
    #     
    #     if msg.topic == 'topic_data_streaming':
    #         # 1. Recibe $Kw/€ del Engine
    #         # 2. (Opcional) Guarda en BBDD para el log
    #         # 3. (El Driver también recibe este msg)
    #         # 4. Si el msg es "FINALIZADO":
    #         #    - Guarda el log final en BBDD
    #         #    - Actualiza estado del CP en BBDD a "ACTIVADO"
    #         pass
    pass

# --- 3. Lógica de Resiliencia ---

def check_cp_heartbeats():
    """Hilo de vigilancia que comprueba los 'last_heartbeat'."""
    print(f"✅ [RESILIENCIA] Vigilante de heartbeats iniciado (Timeout: {HEARTBEAT_TIMEOUT}s)...")
    while True:
        time.sleep(HEARTBEAT_TIMEOUT // 2) # Comprobar cada mitad de timeout
        
        # TODO: Lógica de BBDD
        # 1. Conectarse a BBDD
        # 2. Buscar CPs donde (CURRENT_TIMESTAMP - last_heartbeat) > HEARTBEAT_TIMEOUT
        # 3. Para cada uno, llamar a update_cp_status_in_db(cp_id, "DESCONECTADO")
        # print("[RESILIENCIA] Comprobando heartbeats...")
        pass

# --- 4. Lógica del Dashboard (TUI) ---

def generate_dashboard():
    """Genera la tabla de Rich para el dashboard."""
    # TODO:
    # 1. Conectarse a BBDD
    # 2. Hacer "SELECT * FROM ChargingPoints"
    # 3. Crear una rich.Table con esos datos
    # 4. Aplicar colores según el estado (VERDE, ROJO, GRIS)
    table = Table(title="Panel de Control EVCharging")
    table.add_column("CP ID", style="cyan")
    table.add_column("Ubicación")
    table.add_column("Precio")
    table.add_column("Estado")
    
    # Ejemplo de datos
    table.add_row("CP_001", "C/Italia 5", "0.54 €/kWh", "[green]ACTIVADO[/green]")
    table.add_row("CP_002", "Av. Maissonave", "0.60 €/kWh", "[red]AVERIADO[/red]")
    table.add_row("CP_003", "Playa San Juan", "0.54 €/kWh", "[grey50]DESCONECTADO[/grey50]")
    
    return Panel(table)


# --- Hilo Principal ---

if __name__ == "__main__":
    # TODO: Inicializar la BBDD (llamar a init_db.py si no existe)
    # init_db.create_tables()

    # Iniciar hilo del Servidor de Sockets
    socket_thread = threading.Thread(target=start_socket_server, daemon=True)
    socket_thread.start()

    # Iniciar hilo del Consumidor de Kafka
    kafka_thread = threading.Thread(target=start_kafka_listener, daemon=True)
    kafka_thread.start()

    # Iniciar hilo del Vigilante de Heartbeats
    heartbeat_thread = threading.Thread(target=check_cp_heartbeats, daemon=True)
    heartbeat_thread.start()
    
    # Iniciar el Dashboard TUI en el hilo principal
    print("Iniciando Dashboard...")
    time.sleep(1) # Dar tiempo a que los hilos arranquen

    with Live(generate_dashboard(), refresh_per_second=1) as live:
        try:
            while True:
                # El dashboard se actualiza solo
                # La función generate_dashboard() volverá a leer de la BBDD
                time.sleep(1)
                live.update(generate_dashboard())
        except KeyboardInterrupt:
            print("\n Deteniendo EV_Central...")