
import mariadb
from database import DB_CONFIG

def debug_admin():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("--- BUSCANDO USUARIO 'admin' ---")
        cursor.execute("SELECT id, username, enterprise_id, role_id FROM sys_users WHERE username = 'admin'")
        user = cursor.fetchone()
        
        if not user:
            print("No se encontró el usuario 'admin'. Listando sys_users disponibles:")
            cursor.execute("SELECT id, username, enterprise_id FROM sys_users LIMIT 10")
            for u in cursor.fetchall():
                print(u)
            return

        user_id, username, enterprise_id, role_id = user
        print(f"Usuario Logueado: ID={user_id}, Username={username}, Enterprise={enterprise_id}, RoleID={role_id}")
        
        if enterprise_id != 1:
            print(f"ADVERTENCIA: El usuario 'admin' no está en la empresa 1. Está en la empresa {enterprise_id}")

        print("\n--- ROLES DEL USUARIO ---")
        cursor.execute("""
            SELECT r.id, r.name, r.enterprise_id
            FROM sys_roles r 
            WHERE r.id = ?
        """, (role_id,))
        roles = cursor.fetchall()
        for r in roles:
            print(f"Rol Asignado: {r[1]} (ID: {r[0]}, Ent: {r[2]})")
            
            print("  -> Permisos en DB:")
            cursor.execute("""
                SELECT p.code 
                FROM sys_role_permissions rp 
                JOIN sys_permissions p ON rp.permission_id = p.id 
                WHERE rp.role_id = ?
            """, (r[0],))
            permissions = cursor.fetchall()
            for p in permissions:
                print(f"     - {p[0]}")

        print("\n--- VERIFICANDO JOIN DE PERMISOS (Query de app.py) ---")
        # Reproducimos la query de app.py para ver por qué podría fallar
        cursor.execute("""
            SELECT DISTINCT p.code 
            FROM sys_permissions p
            JOIN sys_role_permissions rp ON p.id = rp.permission_id
            JOIN sys_users u ON u.role_id = rp.role_id AND u.enterprise_id = rp.enterprise_id
            WHERE u.id = ? AND u.enterprise_id = ?
        """, (user_id, enterprise_id))
        perms_app = cursor.fetchall()
        print(f"Permisos que ve la APP: {[p[0] for p in perms_app]}")
        
        if not perms_app:
             print("\n!!! ALERTA: La APP no retorna permisos. Posible causa: Join con Enterprise ID.")
             print("Verificando sys_role_permissions...")
             cursor.execute("SELECT * FROM sys_role_permissions WHERE role_id = ?", (role_id,))
             print(cursor.fetchall())


        print("\n--- DATOS DE LA EMPRESA 1 ---")
        cursor.execute("SELECT COUNT(*) FROM libros WHERE enterprise_id = 1")
        count_libros = cursor.fetchone()[0]
        print(f"Libros en empresa 1: {count_libros}")

        cursor.execute("SELECT COUNT(*) FROM prestamos WHERE enterprise_id = 1")
        count_prestamos = cursor.fetchone()[0]
        print(f"Préstamos en empresa 1: {count_prestamos}")

        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_admin()
