# ev_cp/EV_CP_M.py
import socket
import threading
import time
import argparse
import sys

# --- Variables Globales ---
central_conn = None
central_lock = threading.Lock()
cp_id_global = None
last_reported_status = "DESCONECTADO" # Correcto para el arranque

engine_conn = None
engine_lock = threading.Lock()
status_lock = threading.Lock()      # Lock para 'last_reported_status'

def send_to_central(message_str):
    # ... (Esta función no cambia) ...
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
            return False

def send_to_engine(message_str):
    # ... (Esta función no cambia) ...
    global engine_conn, engine_lock
    with engine_lock:
        if not engine_conn:
            print(f"[{cp_id_global}-Monitor] Error: No conectado a Engine.")
            return False
        try:
            engine_conn.sendall(f"{message_str}\n".encode('utf-8'))
            return True
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"[{cp_id_global}-Monitor] Error fatal al enviar a Engine: {e}")
            return False

def health_check_loop(engine_ip, engine_port, cp_id):
    """
    Un hilo que se conecta al Engine (EV_CP_E) y lo monitorea.
    """
    global last_reported_status, engine_conn, engine_lock, status_lock
    
    while True:
        if not engine_conn:
            # ... (Lógica de reconexión al Engine no cambia) ...
            try:
                print(f"[{cp_id}-Monitor] Conectando a Engine en {engine_ip}:{engine_port}...")
                conn_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn_obj.connect((engine_ip, engine_port))
                conn_obj.sendall(f"ID;{cp_id}\n".encode('utf-8'))
                response = conn_obj.recv(1024).decode('utf-8').strip()
                if response != "ID_OK":
                    conn_obj.close()
                    time.sleep(5)
                    continue
                print(f"[{cp_id}-Monitor] ✅ Conectado y autenticado con Engine.")
                with engine_lock:
                    engine_conn = conn_obj
            except Exception:
                engine_conn = None # Asegurarse de que esté None
            
            if not engine_conn:
                with status_lock:
                    if last_reported_status != "AVERIADO":
                        print(f"[{cp_id}-Monitor] 🚨 Reportando Engine 'AVERIADO' a Central (conexión fallida).")
                        if send_to_central(f"STATUS;AVERIADO"):
                            last_reported_status = "AVERIADO"
                time.sleep(5)
                continue

        # --- Bucle de PING/PONG MODIFICADO ---
        try:
            if not send_to_engine("PING"):
                 raise ConnectionResetError("Fallo al enviar PING a Engine")

            response = engine_conn.recv(1024).decode('utf-8').strip()

            current_status = ""
            if response == "OK":
                current_status = "ACTIVADO"
            elif response == "KO":
                current_status = "AVERIADO"
            else:
                raise ConnectionResetError("Respuesta de Engine inválida")

            # --- LÓGICA DE ESTADO MODIFICADA ---
            with status_lock:
                # Si el Engine dice 'AVERIADO' (KO) pero nosotros estamos en 'PARADO',
                # NO lo reportes. El 'PARADO' administrativo tiene prioridad.
                if current_status == "AVERIADO" and last_reported_status == "PARADO":
                    continue # Ignorar el KO, ya sabemos que está parado
                
                if current_status != last_reported_status:
                    print(f"[{cp_id}-Monitor] 🚨 Estado del Engine ha cambiado a {current_status}. Reportando a Central...")
                    if send_to_central(f"STATUS;{current_status}"):
                        last_reported_status = current_status
                    else:
                        print(f"[{cp_id}-Monitor] ❌ No se pudo reportar el nuevo estado a Central.")
            # --- FIN LÓGICA MODIFICADA ---

        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"[{cp_id}-Monitor] 💔 Conexión perdida con Engine: {e}. Reintentando...")
            with engine_lock:
                if engine_conn:
                    engine_conn.close()
                engine_conn = None
            
            with status_lock: # <-- Añadido lock
                if last_reported_status != "AVERIADO":
                    if send_to_central("STATUS;AVERIADO"):
                         last_reported_status = "AVERIADO"
        
        time.sleep(1)

def central_heartbeat_loop():
    # ... (Esta función no cambia) ...
    while True:
        if send_to_central("HEARTBEAT"):
            pass
        else:
            print(f"[{cp_id_global}-Monitor] 💔 Fallo al enviar heartbeat. El hilo principal reconectará.")
            break 
        time.sleep(10)

def main():
    global central_conn, cp_id_global, last_reported_status, engine_conn, engine_lock, status_lock # <-- Añadido status_lock
    
    parser = argparse.ArgumentParser(description="EV Charging Point - Monitor")
    parser.add_argument('--central-ip', required=True, help="IP de EV_Central")
    parser.add_argument('--central-port', type=int, required=True, help="Puerto de EV_Central")
    parser.add_argument('--engine-ip', required=True, help="IP de EV_CP_E (Engine local)")
    parser.add_argument('--engine-port', type=int, required=True, help="Puerto de EV_CP_E (Engine local)")
    parser.add_argument('--cp-id', required=True, help="ID único de este Charging Point")
    args = parser.parse_args()
    
    cp_id_global = args.cp_id

    health_thread = threading.Thread(
        target=health_check_loop, 
        args=(args.engine_ip, args.engine_port, args.cp_id), 
        daemon=True
    )
    health_thread.start()

    while True: 
        heartbeat_thread = None
        try:
            print(f"[{cp_id_global}-Monitor] Conectando a Central en {args.central_ip}:{args.central_port}...")
            conn_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn_obj.connect((args.central_ip, args.central_port))
            
            with central_lock:
                central_conn = conn_obj
            
            # 1. Registrarse
            # ... (Lógica de registro y precio no cambia) ...
            location = f"C/{args.cp_id.lower()} 123"
            base_price = 0.50
            bonus_per_char = 0.03
            price = base_price + (len(args.cp_id)) * bonus_per_char           
            register_msg = f"REGISTER;{args.cp_id};{location};{price:.2f}"
            
            if not send_to_central(register_msg):
                raise ConnectionError("Fallo al registrarse.")
            
            print(f"[{cp_id_global}-Monitor] ✅ Registrado en Central como {args.cp_id}.")
            
            heartbeat_thread = threading.Thread(target=central_heartbeat_loop, daemon=True)
            heartbeat_thread.start()

            # 3. Bucle de Escucha de Comandos MODIFICADO
            while True:
                data = central_conn.recv(1024)
                if not data:
                    raise ConnectionError("Central se desconectó.")
                
                commands = data.decode('utf-8').strip().split('\n')
                for cmd in commands:
                    if not cmd:
                        continue
                        
                    print(f"[{cp_id_global}-Monitor] 📩 Comando recibido de Central: {cmd}")
                    
                    # --- LÓGICA DE 'STOP' MODIFICADA ---
                    if cmd == "STOP_CP":
                        print(f"[{cp_id_global}-Monitor]   -> 🛑 Central ha ordenado PARAR.")
                        with status_lock:
                            last_reported_status = "PARADO"
                        send_to_engine("FORCE_STOP")
                    
                    elif cmd == "RESUME_CP":
                        print(f"[{cp_id_global}-Monitor]   -> ▶️ Central ha ordenado REANUDAR.")
                        with status_lock:
                            last_reported_status = "ACTIVADO"
                        send_to_engine("FORCE_RESUME")
                    # --- FIN LÓGICA MODIFICADA ---

                    elif cmd.startswith("ACK;"):
                        pass
                    else:
                        print(f"[{cp_id_global}-Monitor]   -> ❓ Comando desconocido.")

        except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError, ConnectionError, OSError) as e:
            # ... (Lógica de reconexión no cambia) ...
            print(f"[{cp_id_global}-Monitor] 💔 Conexión perdida con Central: {e}. Reintentando en 5s...")
            with central_lock:
                if central_conn:
                    central_conn.close()
                central_conn = None
            if heartbeat_thread and heartbeat_thread.is_alive():
                heartbeat_thread.join(1.0)
        
        time.sleep(5)

if __name__ == "__main__":
    main()