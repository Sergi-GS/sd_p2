# ev_cp/EV_CP_M.py
import socket
import threading
import time
import argparse
import sys

# --- Variables Globales ---
central_conn = None                 # Objeto socket para la conexión a Central
central_lock = threading.Lock()     # Lock para proteger el acceso a central_conn
cp_id_global = None                 # ID de este CP
last_reported_status = "ACTIVADO"   # Para evitar enviar reportes duplicados

# --- Comunicación Segura con Central ---

def send_to_central(message_str):
    """
    Envía un mensaje a EV_Central de forma thread-safe.
    Usa el protocolo simple (separado por \n).
    """
    global central_conn, central_lock
    
    with central_lock:
        if not central_conn:
            print(f"[{cp_id_global}-Monitor] Error: No conectado a Central.")
            return False
        
        try:
            central_conn.sendall(f"{message_str}\n".encode('utf-8'))
            return True
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"[{cp_id_global}-Monitor] Error fatal al enviar a Central: {e}")
            # El bucle principal se encargará de cerrar y reconectar
            return False

# --- Hilo 1: Vigilante del Engine ---

def health_check_loop(engine_ip, engine_port, cp_id):
    """
    Un hilo que se conecta al Engine (EV_CP_E) y lo monitorea.
    Reporta fallos a EV_Central.
    """
    global last_reported_status
    engine_conn = None

    while True: # Bucle de reconexión al Engine
        if not engine_conn:
            try:
                print(f"[{cp_id}-Monitor] Conectando a Engine en {engine_ip}:{engine_port}...")
                engine_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                engine_conn.connect((engine_ip, engine_port))
                
                # 1. Enviar ID al Engine (requisito de EV_CP_E)
                engine_conn.sendall(f"ID;{cp_id}\n".encode('utf-8'))
                response = engine_conn.recv(1024).decode('utf-8').strip()
                
                if response != "ID_OK":
                    print(f"[{cp_id}-Monitor] Engine rechazó el ID. Respuesta: {response}. Reintentando...")
                    engine_conn.close()
                    engine_conn = None
                    time.sleep(5)
                    continue
                
                print(f"[{cp_id}-Monitor] ✅ Conectado y autenticado con Engine.")

            except ConnectionRefusedError:
                print(f"[{cp_id}-Monitor] ❌ Engine no encontrado. Reintentando en 5s...")
                engine_conn = None
            except Exception as e:
                print(f"[{cp_id}-Monitor] ❌ Error conectando a Engine: {e}")
                engine_conn = None
            
            if not engine_conn:
                # Si no se pudo conectar, reportar AVERIADO a Central (si no se ha hecho ya)
                if last_reported_status != "AVERIADO":
                    print(f"[{cp_id}-Monitor] 🚨 Reportando Engine 'AVERIADO' a Central (conexión fallida).")
                    if send_to_central(f"STATUS;AVERIADO"):
                        last_reported_status = "AVERIADO"
                time.sleep(5)
                continue # Vuelve al inicio del bucle de reconexión

        # --- Bucle de PING/PONG (mientras esté conectado) ---
        try:
            # Enviar PING
            engine_conn.sendall(b"PING\n")
            response = engine_conn.recv(1024).decode('utf-8').strip()

            current_status = ""
            if response == "OK":
                current_status = "ACTIVADO"
            elif response == "KO":
                current_status = "AVERIADO"
            else:
                # Si la respuesta es inválida, el Engine está roto
                raise ConnectionResetError("Respuesta de Engine inválida")

            # Reportar a Central SOLO si el estado ha cambiado
            if current_status != last_reported_status:
                print(f"[{cp_id}-Monitor] 🚨 Estado del Engine ha cambiado a {current_status}. Reportando a Central...")
                if send_to_central(f"STATUS;{current_status}"):
                    last_reported_status = current_status
                else:
                    print(f"[{cp_id}-Monitor] ❌ No se pudo reportar el nuevo estado a Central.")

        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"[{cp_id}-Monitor] 💔 Conexión perdida con Engine: {e}. Reintentando...")
            engine_conn.close()
            engine_conn = None
            
            # Si se pierde la conexión, reportar AVERIADO
            if last_reported_status != "AVERIADO":
                if send_to_central("STATUS;AVERIADO"):
                     last_reported_status = "AVERIADO"
        
        time.sleep(1) # Ping cada 1 segundo

# --- Hilo 2: Heartbeats para Central ---

def central_heartbeat_loop():
    """
    Un hilo que simplemente envía heartbeats a Central cada 10s.
    """
    while True:
        if send_to_central("HEARTBEAT"):
            # print(f"[{cp_id_global}-Monitor] ❤️  Heartbeat enviado a Central.")
            pass
        else:
            print(f"[{cp_id_global}-Monitor] 💔 Fallo al enviar heartbeat. El hilo principal reconectará.")
            break # El hilo principal se encargará de la reconexión
        
        time.sleep(10) # Frecuencia del heartbeat

# --- Hilo Principal: Conexión a Central y Escucha ---

def main():
    global central_conn, cp_id_global, last_reported_status
    
    parser = argparse.ArgumentParser(description="EV Charging Point - Monitor")
    parser.add_argument('--central-ip', required=True, help="IP de EV_Central")
    parser.add_argument('--central-port', type=int, required=True, help="Puerto de EV_Central")
    parser.add_argument('--engine-ip', required=True, help="IP de EV_CP_E (Engine local)")
    parser.add_argument('--engine-port', type=int, required=True, help="Puerto de EV_CP_E (Engine local)")
    parser.add_argument('--cp-id', required=True, help="ID único de este Charging Point")
    args = parser.parse_args()
    
    cp_id_global = args.cp_id

    # Iniciar el hilo de health check (Hilo 1)
    health_thread = threading.Thread(
        target=health_check_loop, 
        args=(args.engine_ip, args.engine_port, args.cp_id), 
        daemon=True
    )
    health_thread.start()

    # Bucle principal (Conexión a Central)
    while True: # Bucle de reconexión a Central
        heartbeat_thread = None
        try:
            print(f"[{cp_id_global}-Monitor] Conectando a Central en {args.central_ip}:{args.central_port}...")
            conn_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn_obj.connect((args.central_ip, args.central_port))
            
            # Asignar a la variable global
            with central_lock:
                central_conn = conn_obj
            
            # 1. Registrarse
            # (En un sistema real, estos datos vendrían de un config local)
            location = f"C/{args.cp_id.lower()} 123"
            price = 0.50 + (len(args.cp_id) % 10) * 0.01 # Precio simulado
            register_msg = f"REGISTER;{args.cp_id};{location};{price:.2f}"
            
            if not send_to_central(register_msg):
                raise ConnectionError("Fallo al registrarse.")
            
            print(f"[{cp_id_global}-Monitor] ✅ Registrado en Central como {args.cp_id}.")
            last_reported_status = "ACTIVADO" # Resetear estado en cada registro
            
            # 2. Iniciar hilo de Heartbeat (Hilo 2)
            heartbeat_thread = threading.Thread(target=central_heartbeat_loop, daemon=True)
            heartbeat_thread.start()

            # 3. Bucle de Escucha de Comandos (en hilo principal)
            while True:
                data = central_conn.recv(1024)
                if not data:
                    raise ConnectionError("Central se desconectó.")
                
                commands = data.decode('utf-8').strip().split('\n')
                for cmd in commands:
                    if not cmd:
                        continue
                        
                    print(f"[{cp_id_global}-Monitor] 📩 Comando recibido de Central: {cmd}")
                    
                    if cmd == "STOP_CP":
                        print(f"[{cp_id_global}-Monitor]   -> 🛑 Central ha ordenado PARAR.")
                        # (Aquí iría la lógica para enviar "STOP" al Engine, si fuera necesario)
                    elif cmd == "RESUME_CP":
                        print(f"[{cp_id_global}-Monitor]   -> ▶️ Central ha ordenado REANUDAR.")
                        # (Lógica para reanudar el Engine)
                    elif cmd.startswith("ACK;"):
                        pass # Ignorar ACKs de registro y heartbeats
                    else:
                        print(f"[{cp_id_global}-Monitor]   -> ❓ Comando desconocido.")

        except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError, ConnectionError, OSError) as e:
            print(f"[{cp_id_global}-Monitor] 💔 Conexión perdida con Central: {e}. Reintentando en 5s...")
            with central_lock:
                if central_conn:
                    central_conn.close()
                central_conn = None
            
            # Esperar a que el hilo de heartbeat muera (si estaba vivo)
            if heartbeat_thread and heartbeat_thread.is_alive():
                heartbeat_thread.join(1.0) # Esperar max 1s
        
        time.sleep(5) # Esperar 5s antes de reintentar la conexión

if __name__ == "__main__":
    main()