"""
Módulo central_gui.py
Contiene la clase CentralApp (la ventana principal de Tkinter) y el 
widget CPPanel (cada panel individual de la rejilla).

Este módulo está diseñado para ser importado por EV_Central.py.
"""

import tkinter as tk
from tkinter import ttk
from tkinter import font
import queue  # Importante para la comunicación thread-safe

# --- Constantes de Estilo (Basadas en el PDF) ---
COLOR_VERDE = "#4CAF50"      # Activado / Suministrando
COLOR_NARANJA = "#FF9800"   # Parado (Out of Order)
COLOR_ROJO = "#F44336"      # Averiado
COLOR_GRIS = "#9E9E9E"      # Desconectado
COLOR_FONDO_REJILLA = "#333333" # Color oscuro para el fondo
COLOR_TEXTO = "#FFFFFF"         # Texto blanco

class CPPanel(tk.Frame):
    """
    Un widget que representa un único Punto de Carga (CP) en el panel.
    Se autogestiona para cambiar color y texto según su estado.
    """
    def __init__(self, parent, cp_id, location, price):
        super().__init__(parent, borderwidth=2, relief="solid", bg=COLOR_GRIS, padx=5, pady=5)
        
        self.cp_id = cp_id
        
        # --- Widgets Internos ---
        self.lbl_id = tk.Label(self, text=cp_id, bg=COLOR_GRIS, fg=COLOR_TEXTO, font=("Arial", 12, "bold"))
        self.lbl_id.pack(fill="x")
        
        self.lbl_location = tk.Label(self, text=location, bg=COLOR_GRIS, fg=COLOR_TEXTO, font=("Arial", 10))
        self.lbl_location.pack(fill="x")
        
        self.lbl_price = tk.Label(self, text=price, bg=COLOR_GRIS, fg=COLOR_TEXTO, font=("Arial", 10))
        self.lbl_price.pack(fill="x")
        
        # Este label es dinámico: muestra estado (Driver, Out of Order) o nada
        self.lbl_status = tk.Label(self, text="", bg=COLOR_GRIS, fg=COLOR_TEXTO, font=("Arial", 10, "italic"))
        self.lbl_status.pack(fill="x", pady=(5,0))
        
        self.widgets = [self, self.lbl_id, self.lbl_location, self.lbl_price, self.lbl_status]
        
        # Estado inicial Desconectado
        self.update_state("DESCONECTADO")

    def _set_colors(self, color):
        """Helper para cambiar el color de fondo de todos los widgets del panel."""
        for w in self.widgets:
            w.configure(bg=color)

    def update_state(self, state, data=None):
        """
        Actualiza la apariencia del panel basado en el estado.
        'data' es un dict para el estado 'SUMINISTRANDO'.
        """
        try:
            if state == "ACTIVADO":
                self._set_colors(COLOR_VERDE)
                self.lbl_status.configure(text="")
                
            elif state == "SUMINISTRANDO":
                self._set_colors(COLOR_VERDE)
                if data:
                    status_text = (
                        f"Driver {data.get('driver', 'N/A')}\n"
                        f"{data.get('kwh', '0.0')} kWh\n"
                        f"{data.get('eur', '0.00')} €"
                    )
                    self.lbl_status.configure(text=status_text)
                
            elif state == "PARADO":
                self._set_colors(COLOR_NARANJA)
                self.lbl_status.configure(text="Out of Order")
                
            elif state == "AVERIADO":
                self._set_colors(COLOR_ROJO)
                self.lbl_status.configure(text="")
                
            elif state == "DESCONECTADO":
                self._set_colors(COLOR_GRIS)
                self.lbl_status.configure(text="")
                
            else:
                print(f"(GUI) Estado desconocido para {self.cp_id}: {state}")
        except Exception as e:
            print(f"Error actualizando panel {self.cp_id}: {e}")

class CentralApp(tk.Tk):
    """
    Clase principal de la aplicación GUI.
    Debe ser instanciada por EV_Central.py.
    """
    def __init__(self, gui_queue):
        super().__init__()
        self.title("SD EV CHARGING SOLUTION. MONITORIZATION PANEL")
        self.geometry("1000x700")
        self.configure(bg="#212121")

        # Cola para comunicación Thread-safe con el Backend (EV_Central.py)
        self.gui_queue = gui_queue
        
        # Referencia al controlador de backend (se setea con set_controller)
        self.backend_controller = None

        self.cp_panels = {} # Diccionario para acceder fácilmente a cada panel por su ID

        self._create_widgets()

        # Iniciar el poller de la cola
        self.after(100, self._process_queue)

    def set_controller(self, controller):
        """
        Almacena una referencia al objeto del backend (tu instancia de EV_Central)
        para poder llamar a sus métodos (ej. al pulsar un botón).
        """
        self.backend_controller = controller

    def _create_widgets(self):
        # --- 1. Título Superior ---
        title_font = font.Font(family="Arial", size=16, weight="bold")
        lbl_title = tk.Label(
            self, 
            text="*** SD EV CHARGING SOLUTION. MONITORIZATION PANEL ***",
            bg="#0D47A1", 
            fg="white", 
            pady=10,
            font=title_font
        )
        lbl_title.pack(fill="x")

        # --- 2. Frame Principal para la Rejilla de CPs ---
        self.grid_frame = tk.Frame(self, bg=COLOR_FONDO_REJILLA, padx=10, pady=10)
        self.grid_frame.pack(fill="x", expand=False)
        # Configurar 5 columnas (como en Figura 1)
        for i in range(5):
            self.grid_frame.columnconfigure(i, weight=1, uniform="cp_col")

        # --- 3. Panel Inferior (Logs y Controles) ---
        bottom_pane = tk.Frame(self, bg="#212121")
        bottom_pane.pack(fill="both", expand=True, pady=10)
        bottom_pane.columnconfigure(0, weight=1)
        bottom_pane.rowconfigure(0, weight=1)
        bottom_pane.rowconfigure(1, weight=1)

        # --- 3a. Panel de Peticiones (On-Going Requests) ---
        requests_frame = self._create_requests_panel(bottom_pane)
        requests_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=(0, 5))

        # --- 3b. Panel de Mensajes de Aplicación ---
        messages_frame = self._create_messages_panel(bottom_pane)
        messages_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=(5, 0))

        # --- 4. Panel de Administrador (Botones Parar/Reanudar) ---
        admin_frame = self._create_admin_panel(self)
        admin_frame.pack(fill="x", side="bottom", pady=10, padx=10)

    def _create_requests_panel(self, parent):
        req_frame = tk.LabelFrame(
            parent, 
            text=" *** ON_GOING DRIVERS REQUESTS *** ", 
            fg="white", 
            bg="#333333",
            font=("Arial", 11, "bold")
        )        
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("Treeview", 
                        background="#555555", 
                        foreground="white", 
                        fieldbackground="#555555",
                        rowheight=25)
        style.map('Treeview', background=[('selected', '#0D47A1')])
        style.configure("Treeview.Heading", 
                        background="#444444", 
                        foreground="white", 
                        font=("Arial", 10, "bold"))

        # Columnas de la tabla
        cols = ('DATE', 'START TIME', 'User ID', 'CP')
        self.requests_tree = ttk.Treeview(req_frame, columns=cols, show='headings')
        
        for col in cols:
            self.requests_tree.heading(col, text=col)
            self.requests_tree.column(col, width=150, anchor="center")
            
        self.requests_tree.pack(fill="both", expand=True, padx=5, pady=5)
        return req_frame

    def _create_messages_panel(self, parent):
        msg_frame = tk.LabelFrame(
            parent, 
            text=" *** APLICATION MESSAGES *** ", 
            fg="white", 
            bg="#333333",
            font=("Arial", 11, "bold")
        )
        
        self.messages_listbox = tk.Listbox(
            msg_frame, 
            bg="#555555", 
            fg="white", 
            selectbackground="#0D47A1",
            font=("Consolas", 10)
        )
        self.messages_listbox.pack(fill="both", expand=True, padx=5, pady=5)
        return msg_frame

    def _create_admin_panel(self, parent):
        admin_frame = tk.Frame(parent, bg="#333333", pady=5)
        
        tk.Label(admin_frame, text="Control Manual:", fg="white", bg="#333333", font=("Arial", 10, "bold")).pack(side="left", padx=10)
        
        tk.Label(admin_frame, text="CP ID:", fg="white", bg="#333333").pack(side="left", padx=(10, 0))
        self.admin_cp_entry = tk.Entry(admin_frame, width=10)
        self.admin_cp_entry.pack(side="left", padx=5)
        
        btn_parar = tk.Button(admin_frame, text="Parar CP", bg=COLOR_NARANJA, fg="white", command=self._on_parar_cp)
        btn_parar.pack(side="left", padx=5)
        
        btn_reanudar = tk.Button(admin_frame, text="Reanudar CP", bg=COLOR_VERDE, fg="white", command=self._on_reanudar_cp)
        btn_reanudar.pack(side="left", padx=5)
        
        return admin_frame
        
    def load_initial_cps(self, cp_list):
        """
        Puebla la rejilla con los CPs cargados desde la BBDD.
        EV_Central.py debe llamar a este método al arrancar.
        
        'cp_list' debe ser una lista de dicts.
        Ej: [{"id": "ALC1", "loc": "C/Italia 5", "price": "0,54€/kWh", "grid_row": 0, "grid_col": 0}, ...]
        """
        for cp_data in cp_list:
            panel = CPPanel(
                self.grid_frame, 
                cp_data["id"], 
                cp_data["loc"], 
                cp_data["price"]
            )
            panel.grid(
                row=cp_data["grid_row"], 
                column=cp_data["grid_col"], 
                sticky="nsew", 
                padx=5, 
                pady=5
            )
            # Guardamos la referencia al panel
            self.cp_panels[cp_data["id"]] = panel
            # El estado por defecto del panel es "DESCONECTADO"

    # --- Métodos de Actualización de la GUI (Llamados por _process_queue) ---
    
    def _update_cp_state(self, cp_id, state, data=None):
        """Actualiza el panel de un CP específico."""
        if cp_id in self.cp_panels:
            self.cp_panels[cp_id].update_state(state, data)
        else:
            self._add_app_message(f"AVISO: Intento de actualizar CP desconocido: {cp_id}")
            
    def _add_request_log(self, date, start_time, user_id, cp):
        """Añade una fila a la tabla de peticiones."""
        self.requests_tree.insert("", "end", values=(date, start_time, user_id, cp))
        
    def _add_app_message(self, message):
        """Añade un mensaje al log de la aplicación."""
        self.messages_listbox.insert("end", message)
        self.messages_listbox.see("end") # Auto-scroll

    # --- Callbacks de Botones (Llaman al Backend) ---
    
    def _on_parar_cp(self):
        cp_id = self.admin_cp_entry.get()
        if not cp_id:
            self._add_app_message("ERROR: Introduce un CP ID para parar.")
            return
        
        if self.backend_controller:
            # Llama al método correspondiente en EV_Central.py
            self.backend_controller.request_parar_cp(cp_id)
            # self._add_app_message(f"Enviando orden PARAR a {cp_id}...")
        else:
            self._add_app_message("ERROR: Backend controller no está conectado.")

    def _on_reanudar_cp(self):
        cp_id = self.admin_cp_entry.get()
        if not cp_id:
            self._add_app_message("ERROR: Introduce un CP ID para reanudar.")
            return
            
        if self.backend_controller:
            # Llama al método correspondiente en EV_Central.py
            self.backend_controller.request_reanudar_cp(cp_id)
            # self._add_app_message(f"Enviando orden REANUDAR a {cp_id}...")
        else:
            self._add_app_message("ERROR: Backend controller no está conectado.")
        
    # --- Gestión de la Cola (Comunicación Thread-Safe) ---
    
    def _process_queue(self):
        """
        Procesa mensajes de la cola de la GUI.
        Este es el único lugar seguro para actualizar la GUI.
        """
        try:
            while True:
                # Obtener un mensaje de la cola (enviado por EV_Central.py)
                message = self.gui_queue.get_nowait()
                
                msg_type = message[0]
                
                if msg_type == "UPDATE_CP":
                    # ("UPDATE_CP", "CP_ID", "ESTADO", data_dict)
                    _, cp_id, state, data = message
                    self._update_cp_state(cp_id, state, data)
                
                elif msg_type == "ADD_REQUEST":
                    # ("ADD_REQUEST", "fecha", "hora", "user", "cp")
                    _, date, start, user, cp = message
                    self._add_request_log(date, start, user, cp)
                    
                elif msg_type == "ADD_MESSAGE":
                    # ("ADD_MESSAGE", "texto del mensaje")
                    _, msg_text = message
                    self._add_app_message(msg_text)

        except queue.Empty:
            # La cola está vacía, no hay nada que hacer
            pass
        finally:
            # Volver a comprobar la cola en 100ms
            self.after(100, self._process_queue)


# --- Bloque de Test ---
# Esto solo se ejecuta si corres `python ev_central/central_gui.py` directamente
# Te permite probar la GUI sin lanzar todo el backend.
if __name__ == "__main__":
    import threading
    import time

    print("--- EJECUTANDO central_gui.py EN MODO TEST ---")

    class DummyController:
        """Simula el backend EV_Central.py para probar la GUI"""
        def __init__(self, q):
            self.gui_queue = q

        def request_parar_cp(self, cp_id):
            print(f"[DummyController] Recibida orden PARAR para {cp_id}")
            self.gui_queue.put(("UPDATE_CP", cp_id, "PARADO", None))
            self.gui_queue.put(("ADD_MESSAGE", f"{cp_id} out of order"))

        def request_reanudar_cp(self, cp_id):
            print(f"[DummyController] Recibida orden REANUDAR para {cp_id}")
            self.gui_queue.put(("UPDATE_CP", cp_id, "ACTIVADO", None))
            self.gui_queue.put(("ADD_MESSAGE", f"{cp_id} reanudado."))

        def start_simulation(self):
            """Simula eventos del backend."""
            time.sleep(2)
            self.gui_queue.put(("ADD_MESSAGE", "CENTRAL system status OK"))
            
            time.sleep(1)
            # Simular peticiones
            self.gui_queue.put(("ADD_REQUEST", "12/9/25", "10:58", "5", "MAD2"))
            self.gui_queue.put(("ADD_REQUEST", "12/9/25", "9:00", "23", "SEV3"))
            self.gui_queue.put(("ADD_REQUEST", "12/9/25", "9:05", "234", "COR1"))
            
            time.sleep(1)
            # Simular estados de CPs
            self.gui_queue.put(("UPDATE_CP", "ALC1", "ACTIVADO", None))
            self.gui_queue.put(("UPDATE_CP", "ALC3", "PARADO", None))
            
            # Suministrando en MAD2
            data_mad2 = {"driver": "5", "kwh": "10.3", "eur": "6.18"}
            self.gui_queue.put(("UPDATE_CP", "MAD2", "SUMINISTRANDO", data_mad2))
            
            self.gui_queue.put(("UPDATE_CP", "MAD3", "AVERIADO", None))
            self.gui_queue.put(("UPDATE_CP", "MAD1", "DESCONECTADO", None))
            
            # Suministrando en SEV3
            data_sev3 = {"driver": "24", "kwh": "50", "eur": "27"}
            self.gui_queue.put(("UPDATE_CP", "SEV3", "SUMINISTRANDO", data_sev3))
            
            self.gui_queue.put(("UPDATE_CP", "SEV2", "PARADO", None))
            self.gui_queue.put(("UPDATE_CP", "VALB", "DESCONECTADO", None))
            self.gui_queue.put(("UPDATE_CP", "VAL1", "ACTIVADO", None))
            
            # Suministrando en COR1
            data_cor1 = {"driver": "234", "kwh": "20", "eur": "8"}
            self.gui_queue.put(("UPDATE_CP", "COR1", "SUMINISTRANDO", data_cor1))


    # --- Lógica de arranque del Test ---
    
    # 1. Crear la cola
    test_queue = queue.Queue()
    
    # 2. Crear la App y pasarle la cola
    app = CentralApp(test_queue)
    
    # 3. Crear el controlador dummy y pasárselo a la app
    dummy_controller = DummyController(test_queue)
    app.set_controller(dummy_controller)

    # 4. Cargar los CPs iniciales (esto tu EV_Central.py lo leerá de la DB)
    cps_de_test = [
        {"id": "ALC1", "loc": "C/Italia 5", "price": "0,54€/kWh", "grid_row": 0, "grid_col": 0},
        {"id": "ALC3", "loc": "Gran Vía 2", "price": "0,54€/kWh", "grid_row": 0, "grid_col": 1},
        {"id": "MAD2", "loc": "C/Serrano 10", "price": "0,6€/kWh", "grid_row": 0, "grid_col": 2},
        {"id": "MAD3", "loc": "C/Pez 23", "price": "0,48€/kWh", "grid_row": 0, "grid_col": 3},
        {"id": "MAD1", "loc": "C/Arguelles", "price": "0,4€/Kwh", "grid_row": 0, "grid_col": 4},
        {"id": "SEV3", "loc": "C/Sevilla 4", "price": "0,54€/kWh", "grid_row": 1, "grid_col": 0},
        {"id": "SEV2", "loc": "Maestranza", "price": "0,4€/KWh", "grid_row": 1, "grid_col": 1},
        {"id": "VALB", "loc": "Museo Arts", "price": "0,5€/kWh", "grid_row": 1, "grid_col": 2},
        {"id": "VAL1", "loc": "San Javier", "price": "0,48€/kWh", "grid_row": 1, "grid_col": 3},
        {"id": "COR1", "loc": "Mezquita", "price": "0,4€/Kwh", "grid_row": 1, "grid_col": 4},
    ]
    app.load_initial_cps(cps_de_test)
    
    # 5. Iniciar el backend simulado en un hilo
    threading.Thread(target=dummy_controller.start_simulation, daemon=True).start()
    
    # 6. Iniciar la GUI
    app.mainloop()