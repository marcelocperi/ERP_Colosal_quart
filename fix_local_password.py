from database import get_db_cursor
from werkzeug.security import generate_password_hash
import sys

def fix_password():
    print("Iniciando cambio de clave local...")
    
    try:
        with get_db_cursor() as cursor:
            # 1. Buscar o Crear Empresa Carabobo
            print("Buscando empresa 'Carabobo'...")
            cursor.execute("SELECT id, nombre FROM sys_enterprises WHERE nombre LIKE '%Carabobo%'")
            empresa = cursor.fetchone()
            
            ent_id = None
            if empresa:
                ent_id = empresa[0]
                print(f"Empresa encontrada: {empresa[1]} (ID: {ent_id})")
            else:
                # Buscar ID 1 y renombrar si es genérica
                cursor.execute("SELECT id, nombre FROM sys_enterprises WHERE id = 1")
                ent1 = cursor.fetchone()
                if ent1:
                    print(f"Renombrando empresa ID 1 ('{ent1[1]}') a 'Carabobo'...")
                    cursor.execute("UPDATE sys_enterprises SET nombre = 'Carabobo' WHERE id = 1")
                    ent_id = 1
                else:
                    print("Creando empresa 'Carabobo'...")
                    cursor.execute("INSERT INTO sys_enterprises (id, nombre, estado) VALUES (1, 'Carabobo', 'activo')")
                    ent_id = 1
            
            # 2. Actualizar Usuario Admin
            username = 'admin'
            new_pass = 'admin123'
            hashed_pass = generate_password_hash(new_pass)
            
            cursor.execute("SELECT id FROM sys_users WHERE username = ? AND enterprise_id = ?", (username, ent_id))
            user = cursor.fetchone()
            
            if user:
                cursor.execute("UPDATE sys_users SET password_hash = ? WHERE id = ?", (hashed_pass, user[0]))
                print(f"✅ Contraseña actualizada a '{new_pass}' para el usuario '{username}' (ID: {user[0]}).")
            else:
                # Crear Admin
                print(f"Usuario {username} no encontrado. Creándolo...")
                # Buscar rol
                cursor.execute("SELECT id FROM sys_roles WHERE enterprise_id = ? LIMIT 1", (ent_id,))
                role = cursor.fetchone()
                if not role:
                    cursor.execute("INSERT INTO sys_roles (enterprise_id, name) VALUES (?, 'AdminSys')", (ent_id,))
                    role_id = cursor.lastrowid
                    if not role_id: # MariaDB specific if lastrowid fails sometimes? usually works
                        cursor.execute("SELECT id FROM sys_roles WHERE enterprise_id = ? ORDER BY id DESC LIMIT 1", (ent_id,))
                        role_id = cursor.fetchone()[0]
                else:
                    role_id = role[0]

                cursor.execute("""
                    INSERT INTO sys_users (enterprise_id, username, password_hash, role_id, is_active)
                    VALUES (?, ?, ?, ?, 1)
                """, (ent_id, username, hashed_pass, role_id))
                print(f"✅ Usuario '{username}' creado con clave '{new_pass}'.")

    except Exception as e:
        print(f"❌ Error: {e}")
        # Print db config to debug
        from database import DB_CONFIG, DB_TYPE
        print(f"Config: {DB_TYPE} - {DB_CONFIG['host']}:{DB_CONFIG['port']} User: {DB_CONFIG['user']}")

if __name__ == "__main__":
    fix_password()
