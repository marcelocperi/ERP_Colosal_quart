from database import get_db_cursor

try:
    with get_db_cursor() as cursor:
        cursor.execute('''
            SELECT r.name as rol, IFNULL(p.category, "Sin Especificar") as modulo, COUNT(p.id) as cantidad_permisos
            FROM sys_roles r
            LEFT JOIN sys_role_permissions srp ON r.id = srp.role_id
            LEFT JOIN sys_permissions p ON srp.permission_id = p.id
            GROUP BY r.name, p.category
            ORDER BY r.name, p.category
        ''')
        rows = cursor.fetchall()
        print(f"{'Rol':<25} | {'Módulo (Contexto)':<20} | {'Reglas Autorizadas'}")
        print('-'*70)
        for row in rows:
            print(f"{str(row[0]):<25} | {str(row[1]):<20} | {row[2]}")
except Exception as e:
    print(e)
