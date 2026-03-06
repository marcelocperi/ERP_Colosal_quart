import mariadb
from database import DB_CONFIG

def debug_stock():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("--- DIAGNÓSTICO DE STOCK ---")
        # Buscar libros con préstamos activos
        cursor.execute("""
            SELECT l.id, l.nombre, 
                   (SELECT IFNULL(SUM(cantidad), 0) FROM stk_existencias WHERE articulo_id = l.id AND enterprise_id = l.enterprise_id) as total_stock,
                   COUNT(p.id) as prestados
            FROM stk_articulos l
            JOIN prestamos p ON l.id = p.libro_id AND p.enterprise_id = l.enterprise_id
            WHERE p.fecha_devolucion_real IS NULL
            GROUP BY l.id
            LIMIT 5
        """)
        
        rows = cursor.fetchall()
        print(f"{'ID':<5} | {'Libro':<30} | {'Stock BD':<10} | {'Prestados Activos':<15}")
        print("-" * 70)
        
        ids_to_fix = []
        for row in rows:
            lid, nombre, stock, prestados = row
            print(f"{lid:<5} | {nombre[:30]:<30} | {stock:<10} | {prestados:<15}")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_stock()
