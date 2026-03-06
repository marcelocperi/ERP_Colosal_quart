import sys
sys.path.insert(0, 'c:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP')

from database import get_db_cursor

# Verificar préstamos activos con autor Martina
with get_db_cursor() as cursor:
    # Primero, ver todos los préstamos activos
    cursor.execute("""
        SELECT p.id, l.nombre, l.autor, u.nombre, u.apellido 
        FROM prestamos p 
        JOIN libros l ON p.libro_id = l.id AND l.enterprise_id = p.enterprise_id
        JOIN usuarios u ON p.usuario_id = u.id AND u.enterprise_id = p.enterprise_id
        WHERE p.fecha_devolucion_real IS NULL
        LIMIT 10
    """)
    all_loans = cursor.fetchall()
    print(f"\n=== TODOS LOS PRÉSTAMOS ACTIVOS (primeros 10) ===")
    print(f"Total encontrados: {len(all_loans)}")
    for r in all_loans:
        print(f"  ID: {r[0]}, Libro: '{r[1]}', Autor: '{r[2]}', Usuario: {r[3]} {r[4]}")
    
    # Ahora buscar con autor Martina
    cursor.execute("""
        SELECT p.id, l.nombre, l.autor, u.nombre, u.apellido 
        FROM prestamos p 
        JOIN libros l ON p.libro_id = l.id AND l.enterprise_id = p.enterprise_id
        JOIN usuarios u ON p.usuario_id = u.id AND u.enterprise_id = p.enterprise_id
        WHERE p.fecha_devolucion_real IS NULL AND l.autor LIKE ?
    """, ('%Martina%',))
    martina_loans = cursor.fetchall()
    print(f"\n=== PRÉSTAMOS ACTIVOS CON AUTOR 'Martina' ===")
    print(f"Total encontrados: {len(martina_loans)}")
    for r in martina_loans:
        print(f"  ID: {r[0]}, Libro: '{r[1]}', Autor: '{r[2]}', Usuario: {r[3]} {r[4]}")
    
    # Verificar si hay libros de Martina en la base de datos
    cursor.execute("SELECT id, nombre, autor FROM libros WHERE autor LIKE ?", ('%Martina%',))
    martina_books = cursor.fetchall()
    print(f"\n=== LIBROS DE MARTINA EN LA BASE DE DATOS ===")
    print(f"Total encontrados: {len(martina_books)}")
    for r in martina_books:
        print(f"  ID: {r[0]}, Título: '{r[1]}', Autor: '{r[2]}'")
