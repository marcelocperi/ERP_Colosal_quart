
import mariadb
from database import DB_CONFIG

def check_reservations():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # In this system, reservations seem to be handled as 'movimientos_pendientes' of type 'egreso' (based on routes.py analysis)
        # Let's verify if there is a 'reservas' table or if we should rely on 'movimientos_pendientes'
        
        print("Checking tables...")
        cursor.execute("SHOW TABLES LIKE 'reservas'")
        if cursor.fetchone():
            print("Table 'reservas' exists. Analyzing structure...")
            cursor.execute("DESCRIBE reservas")
            for col in cursor.fetchall():
                print(col)
        else:
            print("Table 'reservas' DOES NOT exist. Checking 'movimientos_pendientes'...")
            cursor.execute("DESCRIBE movimientos_pendientes")
            for col in cursor.fetchall():
                print(col)
                
            print("\nSample 'reservas' (pending egresos):")
            cursor.execute("""
                SELECT mp.id, l.nombre, m.nombre as descripcion, mp.fecha_estimada, mp.comentario
                FROM movimientos_pendientes mp
                JOIN stk_articulos l ON mp.libro_id = l.id
                JOIN stk_motivos m ON mp.motivo_id = m.id
                WHERE (mp.tipo = 'egreso' OR mp.tipo = 'SALIDA') AND mp.estado = 'pendiente'
                LIMIT 5
            """)
            print(cursor.fetchall())

        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_reservations()
