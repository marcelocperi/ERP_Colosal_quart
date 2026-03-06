
import mariadb
from database import DB_CONFIG

def debug_admin():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("--- DEBUGGING USER 'admin' ---")
        cursor.execute("SELECT id, username, enterprise_id, role_id FROM sys_users WHERE username = 'admin'")
        user = cursor.fetchone()
        
        if not user:
            print("User 'admin' NOT FOUND.")
            return

        user_id, username, enterprise_id, role_id = user
        print(f"ID: {user_id}")
        print(f"Username: '{username}'")
        print(f"EnterpriseID: {enterprise_id}")
        print(f"RoleID: {role_id}")
        
        cursor.execute("SELECT name FROM sys_roles WHERE id = ?", (role_id,))
        role_row = cursor.fetchone()
        role_name = role_row[0] if role_row else "None"
        print(f"RoleName: '{role_name}'")

        # Check conditions from app.py
        username_clean = str(username).strip().lower()
        role_clean = str(role_name).strip()
        role_lower = role_clean.lower()
        
        is_id_1 = (int(user_id) == 1)
        is_username_admin = (username_clean == 'admin')
        is_admin_role = role_lower in ['admin', 'administrador', 'administrator']
        
        print(f"Condition is_id_1: {is_id_1}")
        print(f"Condition is_username_admin: {is_username_admin}")
        print(f"Condition is_admin_role: {is_admin_role}")
        
        should_have_all = is_username_admin or is_admin_role or is_id_1
        print(f"Should have 'all' permission: {should_have_all}")
        
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_admin()
