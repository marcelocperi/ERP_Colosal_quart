import mariadb
from database import DB_CONFIG

def fix_stock_model():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("--- CORRIGIENDO MODELO DE STOCK (Disponible -> Total) ---")
        
        # 1. Obtener libros con préstamos activos
        cursor.execute("""
            SELECT l.id, l.nombre, l.numero_ejemplares, COUNT(p.id) as prestados_activos
            FROM libros l
            JOIN prestamos p ON l.id = p.libro_id
            WHERE p.fecha_devolucion_real IS NULL
            GROUP BY l.id
        """)
        
        rows = cursor.fetchall()
        count = 0
        for row in rows:
            lid, nombre, actual, prestados = row
            nuevo_total = actual + prestados
            
            print(f"Update Libro {lid} ({nombre[:15]}...): Stock {actual} + {prestados} Prestados = {nuevo_total}")
            
            # Actualizar a Stock Total
            cursor.execute("UPDATE libros SET numero_ejemplares = ? WHERE id = ?", (nuevo_total, lid))
            count += 1
            
        conn.commit()
        print(f"Finalizado. {count} libros actualizados.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fix_stock_model()
