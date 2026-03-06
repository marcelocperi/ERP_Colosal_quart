
from database import get_db_cursor

def setup():
    # Asumimos enterprise_id = 1 para la configuración inicial, o buscamos la primera
    with get_db_cursor() as cursor:
        cursor.execute("SELECT id FROM sys_enterprises LIMIT 1")
        ent_row = cursor.fetchone()
        if not ent_row:
            print("No se encontró empresa")
            return
        enterprise_id = ent_row[0]

        # 1. Asegurar Permiso de Auditoría
        cursor.execute("SELECT id FROM sys_permissions WHERE code='view_permission_audit'")
        p_row = cursor.fetchone()
        if not p_row:
            cursor.execute("INSERT INTO sys_permissions (enterprise_id, code, description, category) VALUES (0, 'view_permission_audit', 'Ver Auditoría de Cambios en Permisos', 'AUDITORIA')")
            p_id = cursor.lastrowid
        else:
            p_id = p_row[0]
        
        # 2. Asegurar Rol Auditor
        cursor.execute("SELECT id FROM sys_roles WHERE name='AUDITOR' AND enterprise_id=%s", (enterprise_id,))
        role_row = cursor.fetchone()
        if not role_row:
            cursor.execute("INSERT INTO sys_roles (enterprise_id, name, description) VALUES (%s, 'AUDITOR', 'Perfil de Supervisión y Auditoría de Controles')", (enterprise_id, ))
            role_id = cursor.lastrowid
        else:
            role_id = role_row[0]
        
        # 3. Asignar permiso al Auditor
        try:
            cursor.execute("INSERT INTO sys_role_permissions (enterprise_id, role_id, permission_id) VALUES (%s, %s, %s)", (enterprise_id, role_id, p_id))
        except Exception as e:
            print(f"Nota: {e}")
            
        print(f"Rol Auditor (ID {role_id}) configurado con acceso a auditoría de permisos.")

if __name__ == "__main__":
    setup()
