import sys
sys.path.insert(0, 'c:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP')

from database import get_db_cursor

# Simular la búsqueda exacta que hace el backend
enterprise_id = 1  # Asumiendo enterprise_id = 1, ajustar si es diferente
autor = "Martina Berthold"

base_query = """
    SELECT p.id, u.nombre, u.apellido, l.nombre, p.fecha_prestamo, p.fecha_devol_esperada,
           u.email, l.isbn, l.autor, u.telefono, l.editorial
    FROM prestamos p
    JOIN usuarios u ON p.usuario_id = u.id AND u.enterprise_id = p.enterprise_id
    JOIN libros l ON p.libro_id = l.id AND l.enterprise_id = p.enterprise_id
    WHERE p.fecha_devolucion_real IS NULL AND p.enterprise_id = ?
"""

conditions = []
params = [enterprise_id]

if autor:
    conditions.append("l.autor LIKE ?")
    params.append(f"%{autor}%")

if conditions:
    base_query += " AND " + " AND ".join(conditions)

base_query += " ORDER BY p.fecha_prestamo DESC LIMIT 100"

print(f"=== SIMULANDO BÚSQUEDA DEL BACKEND ===")
print(f"Query: {base_query}")
print(f"Params: {params}")
print()

with get_db_cursor() as cursor:
    cursor.execute(base_query, tuple(params))
    rows = cursor.fetchall()
    
    print(f"Resultados encontrados: {len(rows)}")
    print()
    
    for r in rows:
        print(f"ID: {r[0]}")
        print(f"  Usuario: {r[1]} {r[2]}")
        print(f"  Libro: {r[3]}")
        print(f"  Autor: {r[8]}")
        print(f"  Email: {r[6]}")
        print(f"  Fecha préstamo: {r[4]}")
        print()
