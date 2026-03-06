import sqlite3
import os
from werkzeug.security import generate_password_hash

DB_NAME = os.environ.get("DB_NAME", "multi_mcp.db")

def reset_password():
    print(f"Conectando a {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. Buscar Empresa Carabobo
    print("Buscando empresa 'Carabobo'...")
    cursor.execute("SELECT id, nombre FROM sys_enterprises WHERE nombre LIKE ?", ('%Carabobo%',))
    empresa = cursor.fetchone()
    
    if not empresa:
        print("❌ No se encontró la empresa 'Carabobo'.")
        print("Empresas disponibles:")
        cursor.execute("SELECT id, nombre FROM sys_enterprises")
        for ent in cursor.fetchall():
            print(f"ID: {ent[0]} - Nombre: {ent[1]}")
            
        # Opcional: Renombrar ID 1 a Carabobo si el usuario quiere?
        confirm = input("\n¿Desea renombrar la empresa ID 1 a 'Carabobo'? (s/n): ")
        if confirm.lower() == 's':
            cursor.execute("UPDATE sys_enterprises SET nombre = 'Carabobo' WHERE id = 1")
            conn.commit()
            print("✅ Empresa ID 1 renombrada a 'Carabobo'.")
            empresa = (1, 'Carabobo')
        else:
            return

    ent_id = empresa[0]
    print(f"Empresa encontrada: {empresa[1]} (ID: {ent_id})")
    
    # 2. Buscar Usuario Admin
    username = 'admin'
    cursor.execute("SELECT id FROM sys_users WHERE username = ? AND enterprise_id = ?", (username, ent_id))
    user = cursor.fetchone()
    
    new_pass = "admin123"
    hashed_pass = generate_password_hash(new_pass)
    
    if user:
        # Update
        cursor.execute("UPDATE sys_users SET password_hash = ? WHERE id = ?", (hashed_pass, user[0]))
        print(f"✅ Contraseña actualizada para usuario '{username}' en empresa '{empresa[1]}'.")
    else:
        # Create
        print(f"⚠️ Usuario '{username}' no encontrado. Creándolo...")
        # Get Role ID
        cursor.execute("SELECT id FROM sys_roles WHERE enterprise_id = ? AND name = 'AdminSys'", (ent_id,))
        role = cursor.fetchone()
        if not role:
            cursor.execute("INSERT INTO sys_roles (enterprise_id, name) VALUES (?, 'AdminSys')", (ent_id,))
            role_id = cursor.lastrowid
        else:
            role_id = role[0]
            
        cursor.execute("""
            INSERT INTO sys_users (enterprise_id, username, password_hash, role_id, is_active)
            VALUES (?, ?, ?, ?, 1)
        """, (ent_id, username, hashed_pass, role_id))
        print(f"✅ Usuario '{username}' creado exitosamente con clave '{new_pass}'.")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    reset_password()
