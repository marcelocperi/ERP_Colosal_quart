import sys
sys.path.insert(0, 'c:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP')

from database import get_db_cursor

# Verificar permisos del usuario admin
with get_db_cursor() as cursor:
    cursor.execute("""
        SELECT u.id, u.username, r.nombre as rol
        FROM usuarios u
        JOIN core_roles r ON u.rol_id = r.id
        WHERE u.username = 'admin' LIMIT 1
    """)
    user = cursor.fetchone()
    
    if user:
        print(f"Usuario: {user[1]} (ID: {user[0]})")
        print(f"Rol: {user[2]}")
        
        cursor.execute("""
            SELECT p.codigo, p.nombre
            FROM core_permisos p
            JOIN core_rol_permisos rp ON p.id = rp.permiso_id
            JOIN usuarios u ON u.rol_id = rp.rol_id
            WHERE u.username = 'admin'
            ORDER BY p.codigo
        """)
        permisos = cursor.fetchall()
        
        print(f"\nPermisos totales: {len(permisos)}")
        
        # Buscar permisos específicos de artículos
        books_perms = [p for p in permisos if 'book' in p[0] or 'stock' in p[0] or 'all' in p[0]]
        if books_perms:
            print("\n✓ Permisos relevantes:")
            for p in books_perms:
                print(f"  - {p[0]}: {p[1]}")
        else:
            print("\n✗ NO tiene permisos de artículos/stock")
            print("\nPrimeros 10 permisos:")
            for p in permisos[:10]:
                print(f"  - {p[0]}: {p[1]}")
    else:
        print("✗ Usuario 'admin' no encontrado")
