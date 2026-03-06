
from database import get_db_cursor

def assign():
    with get_db_cursor() as cursor:
        # 1. Obtener ID del rol Auditor
        cursor.execute("SELECT id FROM sys_roles WHERE name='AUDITOR' LIMIT 1")
        auditor_row = cursor.fetchone()
        if not auditor_row:
            print("Error: Rol AUDITOR no encontrado.")
            return
        auditor_id = auditor_row[0]

        # 2. Obtener ID del permiso
        cursor.execute("SELECT id FROM sys_permissions WHERE code='view_permission_audit' LIMIT 1")
        perm_row = cursor.fetchone()
        if not perm_row:
            print("Error: Permiso view_permission_audit no encontrado.")
            return
        perm_id = perm_row[0]

        # 3. Asignar permiso también al rol adminSys para que superadmin lo vea sin cambiar de rol si quiere
        cursor.execute("SELECT id FROM sys_roles WHERE name='adminSys' LIMIT 1")
        admin_row = cursor.fetchone()
        if admin_row:
            admin_role_id = admin_row[0]
            try:
                # Obtenemos enterprise_id de adminSys
                cursor.execute("SELECT enterprise_id FROM sys_roles WHERE id=%s", (admin_role_id,))
                ent_row = cursor.fetchone()
                ent_id = ent_row[0] if ent_row else 1
                
                cursor.execute("INSERT INTO sys_role_permissions (enterprise_id, role_id, permission_id) VALUES (%s, %s, %s)", (ent_id, admin_role_id, perm_id))
                print(f"Permiso de auditoría añadido al rol adminSys (ID {admin_role_id})")
            except:
                print("Nota: El rol adminSys ya tenía o no pudo recibir el permiso.")

        # 4. Asignar rol Auditor a superadmin (según pedido literal)
        cursor.execute("UPDATE sys_users SET role_id = %s WHERE username = 'superadmin'", (auditor_id,))
        print(f"Usuario superadmin ahora tiene el rol AUDITOR (ID {auditor_id})")

if __name__ == "__main__":
    assign()
