from database import get_db_cursor

def setup_cost_accounting_permission():
    print("Iniciando setup de permiso cost_accounting...")
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Verificar si existe el permiso
        cursor.execute("SELECT id FROM sys_permissions WHERE code = %s", ('cost_accounting',))
        perm = cursor.fetchone()
        
        if not perm:
            print("Insertando nuevo permiso: cost_accounting")
            cursor.execute("""
                INSERT INTO sys_permissions (code, description, category, enterprise_id)
                VALUES (%s, %s, %s, %s)
            """, ('cost_accounting', 'Aprobar propuestas de cambios de precios', 'PRICING', 0))
            perm_id = cursor.lastrowid
        else:
            perm_id = perm['id']
            print(f"El permiso ya existe (ID: {perm_id})")

        # 2. Gestionar Roles
        cursor.execute("SELECT id, name, enterprise_id FROM sys_roles WHERE name LIKE %s OR name = %s", ('%Admin%', 'ANALISTA_COSTOS'))
        roles = cursor.fetchall()
        
        # Si no existe ANALISTA_COSTOS, crearlo
        if not any(r['name'] == 'ANALISTA_COSTOS' for r in roles):
             print("Creando rol ANALISTA_COSTOS (SOD Control)...")
             cursor.execute("""
                INSERT INTO sys_roles (enterprise_id, name, description) 
                VALUES (0, 'ANALISTA_COSTOS', 'Responsable de la gestión de costos y aprobación de listas de precios')
             """)
             cursor.execute("SELECT id, name, enterprise_id FROM sys_roles WHERE name = 'ANALISTA_COSTOS'")
             roles.append(cursor.fetchone())

        # 3. Asignar permiso a los roles
        for role in roles:
            cursor.execute("SELECT 1 FROM sys_role_permissions WHERE role_id = %s AND permission_id = %s", (role['id'], perm_id))
            if not cursor.fetchone():
                print(f"Asignando cost_accounting al rol: {role['name']} (Enterprise: {role['enterprise_id']})")
                cursor.execute("""
                    INSERT INTO sys_role_permissions (role_id, permission_id, enterprise_id) 
                    VALUES (%s, %s, %s)
                """, (role['id'], perm_id, role['enterprise_id']))
            else:
                print(f"El rol {role['name']} ya tiene el permiso.")

    print("✅ Proceso completado con éxito.")

if __name__ == "__main__":
    setup_cost_accounting_permission()
