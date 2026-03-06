from database import get_db_cursor

def setup_cost_accounting_permission():
    print("Iniciando setup de permiso cost_accounting...")
    with get_db_cursor() as cursor:
        # 1. Verificar si existe el permiso
        cursor.execute("SELECT id FROM sys_permissions WHERE code = 'cost_accounting'")
        perm = cursor.fetchone()
        
        if not perm:
            print("Insertando nuevo permiso: cost_accounting")
            cursor.execute("""
                INSERT INTO sys_permissions (module, code, description, active)
                VALUES ('PRICING', 'cost_accounting', 'Permiso para aprobar propuestas de cambios de precios (Pricing Approval)', 1)
            """)
            perm_id = cursor.lastrowid
        else:
            perm_id = perm[0]
            print(f"El permiso ya existe (ID: {perm_id})")

        # 2. Asignar el permiso a los roles Admin y ANALISTA_COSTOS (si existen)
        # Buscamos roles que tengan 'Admin' en el nombre para el SuperAdmin
        cursor.execute("SELECT id, name FROM sys_roles WHERE name LIKE '%Admin%'")
        roles_admin = cursor.fetchall()
        
        for role in roles_admin:
            # Verificar si ya tiene la asignación
            cursor.execute("SELECT 1 FROM sys_roles_permissions WHERE role_id = %s AND permission_id = %s", (role[0], perm_id))
            if not cursor.fetchone():
                print(f"Asignando permiso cost_accounting al rol: {role[1]}")
                cursor.execute("INSERT INTO sys_roles_permissions (role_id, permission_id) VALUES (%s, %s)", (role[0], perm_id))
            else:
                print(f"El rol {role[1]} ya tiene el permiso.")

    print("Setup finalizado con éxito.")

if __name__ == "__main__":
    setup_cost_accounting_permission()
