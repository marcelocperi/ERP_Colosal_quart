
import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    # Get 5 books ready for scanning
    cursor.execute("""
        SELECT codigo as isbn, nombre 
        FROM stk_articulos 
        WHERE enterprise_id = 1 
        AND api_checked = 0 
        AND codigo IS NOT NULL 
        LIMIT 5
    """)
    
    libros = cursor.fetchall()
    
    if not libros:
        print("No hay libros pendientes de escanear.")
    else:
        print("\nLibros listos para escanear con Cuspide:\n")
        for i, l in enumerate(libros, 1):
            isbn = l['isbn'] if l['isbn'] else 'Sin ISBN'
            titulo = l['nombre'] if l['nombre'] else 'Sin titulo'
            print(f"{i}. ISBN: {isbn}")
            print(f"   Titulo: {titulo}\n")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
