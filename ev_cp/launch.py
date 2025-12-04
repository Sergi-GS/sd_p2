


import os
import time
import sys


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


CENTRAL_IP = "localhost"  # IP de (Central/Kafka) 
CENTRAL_PORT = "8000"
KAFKA_BROKER = "172.20.243.110:9092"
ENGINE_IP = "localhost" 

CP_IDS = ["CRT1", "MAD1", "ALC1"] # Cantidad y ID de CP a levantar
BASE_PORT = 6000 
NUM_CPS = len(CP_IDS)

# Determina el ejecutable de Python
PYTHON_EXECUTABLE = sys.executable 
if not PYTHON_EXECUTABLE:
    PYTHON_EXECUTABLE = 'python'

def build_engine_command(cp_id, port):
    engine_path = os.path.join(PROJECT_ROOT, "EV_CP_E.py")
    
    engine_cmd = (
        f'"{PYTHON_EXECUTABLE}" "{engine_path}" '  
        f"--socket-port {port} "
        f"--kafka-broker {KAFKA_BROKER}"
    )
    return f'start "CP {cp_id} -  ENGINE {port}" cmd /k "{engine_cmd}"'

def build_monitor_command(cp_id, port):
    monitor_path = os.path.join(PROJECT_ROOT, "EV_CP_M.py")
    
    monitor_cmd = (
        f'"{PYTHON_EXECUTABLE}" "{monitor_path}" ' 
        f"--cp-id {cp_id} "
        f"--central-ip {CENTRAL_IP} "
        f"--central-port {CENTRAL_PORT} "
        f"--engine-ip {ENGINE_IP} "
        f"--engine-port {port}"
    )
    return f'start "CP {cp_id} - 🔌 MONITOR" cmd /k "{monitor_cmd}"'

if __name__ == "__main__":
    
    print(f"--- INICIANDO {NUM_CPS} PARES CP EN {NUM_CPS * 2} VENTANAS SEPARADAS ---")
    
    for i in range(NUM_CPS):
        cp_id = CP_IDS[i]
        port = BASE_PORT + i + 1 
        
        
        engine_launch_cmd = build_engine_command(cp_id, port)
        os.system(engine_launch_cmd)
      
        time.sleep(1) 
        monitor_launch_cmd = build_monitor_command(cp_id, port)
        os.system(monitor_launch_cmd)
        
        time.sleep(1) 
    print("\n[LAUNCHER] Proceso de lanzamiento finalizado.")