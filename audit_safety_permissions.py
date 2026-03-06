from database import get_db_cursor
import json

def verify_and_fix_permissions():
    print("--- Auditoría de Control de Accesos (SoD/CISA) ---")
    
    # ID de empresa y usuario para auditoría
    ENT_ID = 0
    USER_ID = 1

    with get_db_cursor(dictionary=True) as cursor:
        # 1. Verificar Permiso de Seguridad Industrial
        cursor.execute("SELECT id FROM sys_permissions WHERE code = 'industrial_safety' AND enterprise_id = %s", (ENT_ID,))
        perm_is = cursor.fetchone()
        if not perm_is:
            print("⚠️ Permiso 'industrial_safety' no existía. Creando...")
            cursor.execute("""
                INSERT INTO sys_permissions (code, description, category, enterprise_id, user_id) 
                VALUES ('industrial_safety', 'Gestionar normativas de seguridad y etiquetas SGA/GHS', 'STOCK', %s, %s)
            """, (ENT_ID, USER_ID))
            perm_is_id = cursor.lastrowid
        else:
            perm_is_id = perm_is['id']
            print(f"✅ Permiso 'industrial_safety' verificado (ID: {perm_is_id})")

        # 2. Verificar Permiso de Bypass (Control SoD)
        cursor.execute("SELECT id FROM sys_permissions WHERE code = 'safety_bypass' AND enterprise_id = %s", (ENT_ID,))
        perm_bypass = cursor.fetchone()
        if not perm_bypass:
            print("⚠️ Creando permiso 'safety_bypass'...")
            cursor.execute("""
                INSERT INTO sys_permissions (code, description, category, enterprise_id, user_id) 
                VALUES ('safety_bypass', 'Autorizar bypass de incompatibilidad química (SoD)', 'STOCK', %s, %s)
            """, (ENT_ID, USER_ID))
            perm_bypass_id = cursor.lastrowid
        else:
            perm_bypass_id = perm_bypass['id']
            print(f"✅ Permiso 'safety_bypass' verificado (ID: {perm_bypass_id})")

        # 3. Verificar Rol "Seguridad Industrial"
        cursor.execute("SELECT id FROM sys_roles WHERE name = 'Seguridad Industrial' AND enterprise_id = %s", (ENT_ID,))
        role = cursor.fetchone()
        if not role:
            print("⚠️ Creando Rol 'Seguridad Industrial'...")
            cursor.execute("""
                INSERT INTO sys_roles (name, description, enterprise_id, user_id) 
                VALUES ('Seguridad Industrial', 'Responsable de normativas GHS y estibaje seguro', %s, %s)
            """, (ENT_ID, USER_ID))
            role_id = cursor.lastrowid
        else:
            role_id = role['id']
            print(f"✅ Rol 'Seguridad Industrial' verificado (ID: {role_id})")

        # 4. Vincular Permisos al Rol
        for pid in [perm_is_id, perm_bypass_id]:
            cursor.execute("""
                SELECT * FROM sys_role_permissions 
                WHERE role_id = %s AND permission_id = %s AND enterprise_id = %s
            """, (role_id, pid, ENT_ID))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO sys_role_permissions (role_id, permission_id, enterprise_id, user_id) 
                    VALUES (%s, %s, %s, %s)
                """, (role_id, pid, ENT_ID, USER_ID))
                print(f"🔗 Permiso ID {pid} vinculado al Rol 'Seguridad Industrial'.")

        # 5. Admin Grant (SoD Validation)
        cursor.execute("SELECT id FROM sys_roles WHERE id = 1 AND enterprise_id = %s LIMIT 1", (ENT_ID,))
        admin_role = cursor.fetchone()
        if admin_role:
             for pid in [perm_is_id, perm_bypass_id]:
                 cursor.execute("""
                    SELECT * FROM sys_role_permissions 
                    WHERE role_id = %s AND permission_id = %s AND enterprise_id = %s
                 """, (admin_role['id'], pid, ENT_ID))
                 if not cursor.fetchone():
                     cursor.execute("""
                        INSERT INTO sys_role_permissions (role_id, permission_id, enterprise_id, user_id) 
                        VALUES (%s, %s, %s, %s)
                     """, (admin_role['id'], pid, ENT_ID, USER_ID))
                     print(f"🛡️ Admin Grant: Permiso ID {pid} vinculado al Administrador.")

    print("--- Auditoría Finalizada con Éxito ---")

if __name__ == "__main__":
    verify_and_fix_permissions()
