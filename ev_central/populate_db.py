import sqlite3

DB_NAME = 'ev_central.db'

cps = [
    # (cp_id, location, price_kwh)
    ("ALC1", "Plaza de los Luceros, 5", 0.60),
    ("MAD1", "Calle Gran Vía, 30", 0.65),
    ("BCN1", "Paseo de Gracia, 22", 0.63),
    ("SEV3", "Avenida de la Constitución, 10", 0.59),
]

def populate_data():
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        print("Añadiendo CPs realistas a la base de datos...")
        
        for cp in cps:
            try:
                # Intenta insertar el nuevo CP
                cursor.execute(
                    "INSERT INTO ChargingPoints (cp_id, location, price_kwh) VALUES (?, ?, ?)",
                    (cp[0], cp[1], cp[2])
                )
                print(f"  > CP '{cp[0]}' creado con éxito.")
            
            except sqlite3.IntegrityError:
                # Si ya existe (por 'cp_id'), lo actualiza (UPDATE)
                cursor.execute(
                    "UPDATE ChargingPoints SET location = ?, price_kwh = ? WHERE cp_id = ?",
                    (cp[1], cp[2], cp[0])
                )
                print(f"  > CP '{cp[0]}' ya existía, actualizado con éxito.")

        conn.commit()
        print("\n¡Base de datos poblada/actualizada!")

    except sqlite3.Error as e:
        print(f"Error al poblar la base de datos: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    # Primero, asegúrate de que las tablas existen
    import init_db
    init_db.create_tables()
    
    # Segundo, puebla los datos
    populate_data()