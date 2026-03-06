from database import get_db_cursor

def setup_cost_accounting_permission():
    print("Iniciando setup de permiso cost_accounting...")
    with get_db_cursor(dictionary=True) as cursor:
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
            perm_id = perm['id']
            print(f"El permiso ya existe (ID: {perm_id})")

        # 2. Asignar el permiso a los roles Admin y ANALISTA_COSTOS (si existen)
        cursor.execute("SELECT id, name FROM sys_roles WHERE name LIKE '%Admin%' OR name = 'ANALISTA_COSTOS'")
        roles = cursor.fetchall()
        
        if not any(r['name'] == 'ANALISTA_COSTOS' for r in roles):
             # Crear rol ANALISTA_COSTOS si no existe para cumplir con SOD
             print("Creando rol ANALISTA_COSTOS para Segregación de Funciones (SOD)...")
             cursor.execute("INSERT INTO sys_roles (enterprise_id, name, description) VALUES (0, 'ANALISTA_COSTOS', 'Responsable de revisar y aprobar cambios de precios')")
             cursor.execute("SELECT id, name FROM sys_roles WHERE name = 'ANALISTA_COSTOS'")
             roles.append(cursor.fetchone())

        for role in roles:
            # Verificar si ya tiene la asignación
            cursor.execute("SELECT 1 FROM sys_roles_permissions WHERE role_id = %s AND permission_id = %s", (role['id'], perm_id))
            if not cursor.fetchone():
                print(f"Asignando permiso cost_accounting al rol: {role['name']}")
                cursor.execute("INSERT INTO sys_roles_permissions (role_id, permission_id) VALUES (%s, %s)", (role['id'], perm_id))
            else:
                print(f"El rol {role['name']} ya tiene el permiso.")

    print("Setup finalizado con éxito.")

if __name__ == "__main__":
    setup_cost_accounting_permission()
