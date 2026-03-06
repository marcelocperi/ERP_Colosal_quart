import sqlite3
import os

DB_NAME = "multi_mcp.db"

def parse_sql(sql_file):
    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Very basic cleanup for SQLite compatibility
    # 1. Remove INT AUTO_INCREMENT
    content = content.replace("INT AUTO_INCREMENT PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
    # 2. Remove ENGINE=InnoDB...
    import re
    content = re.sub(r'ENGINE=InnoDB.*?;', ';', content, flags=re.DOTALL)
    # 3. Remove default charset
    content = re.sub(r'DEFAULT CHARSET=.*?;', ';', content, flags=re.DOTALL)
    # 4. Fix TIMESTAMP
    content = content.replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "DATETIME DEFAULT CURRENT_TIMESTAMP")
    # 5. Remove 'COLLATE' which sqlite might not like in visual mode
    # content = content.replace("COLLATE utf8mb4_uca1400_ai_ci", "") # handled by step 3 regex mostly

    # Split commands
    commands = content.split(';')
    return [c.strip() for c in commands if c.strip()]

def setup_sqlite():
    print(f"Creando base de datos SQLite: {DB_NAME}")
    
    if os.path.exists(DB_NAME):
        print(f"¡Atención! La base de datos {DB_NAME} ya existe. Se agregarán tablas si faltan.")
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. Load ERP Schema
    print("Cargando esquema ERP...")
    try:
        commands = parse_sql('erp_schema.sql')
        for cmd in commands:
            try:
                cursor.execute(cmd)
            except sqlite3.OperationalError as e:
                if "already exists" in str(e):
                    pass # Table exists, ignore
                else:
                    print(f"Error en ERP Schema: {e}")
                    # print(cmd)
    except Exception as e:
        print(f"No se pudo leer erp_schema.sql: {e}")

    # 2. Load Users Schema (from txt? or just manually create)
    # The file usuarios_create.txt exists. Let's use it.
    print("Cargando esquema Usuarios...")
    try:
        commands = parse_sql('usuarios_create.txt')
        for cmd in commands:
            # Fix MySQL specific int(11) which sqlite allows but clean is better
            cmd = cmd.replace("int(11)", "INTEGER").replace("AUTO_INCREMENT", "AUTOINCREMENT")
            # Remove MySQL constraints inside create table if they are complex? 
            # SQLite supports most CONSTRAINT syntax now.
            
            # Simple fix for the specific CREATE TABLE `usuarios` syntax
            cmd = cmd.replace("`", "") # Remove backticks
            
            try:
                cursor.execute(cmd)
            except sqlite3.OperationalError as e:
                if "already exists" in str(e):
                    pass
                else:
                    print(f"Error en Usuarios Schema: {e}")
                    # print(cmd)
    except Exception as e:
        print(f"No se pudo leer usuarios_create.txt: {e}")

    # 3. Create basic data
    print("Insertando datos iniciales...")
    try:
        # Empresas
        cursor.execute("INSERT OR IGNORE INTO sys_enterprises (id, nombre, estado) VALUES (1, 'Carabobo', 'activo')")
        # Roles
        cursor.execute("INSERT OR IGNORE INTO sys_roles (id, enterprise_id, name) VALUES (1, 1, 'AdminSys')")
        
        # User Admin
        # Need hash
        from werkzeug.security import generate_password_hash
        pw_hash = generate_password_hash("admin123")
        
        # Check if admin exists
        cursor.execute("SELECT id FROM sys_users WHERE username='admin'")
        if not cursor.fetchone():
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                enterprise_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                password_hash TEXT,
                email TEXT,
                role_id INTEGER,
                recovery_attempts INTEGER DEFAULT 0,
                temp_password_hash TEXT,
                temp_password_expires DATETIME,
                temp_password_used INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            cursor.execute("INSERT INTO sys_users (enterprise_id, username, password_hash, role_id) VALUES (1, 'admin', ?, 1)", (pw_hash,))
            print("Usuario 'admin' creado (Pass: admin123 para Carabobo)")
            
    except Exception as e:
        print(f"Error insertando datos: {e}")
        
    conn.commit()
    conn.close()
    print("¡Base de datos SQLite lista!")

if __name__ == "__main__":
    setup_sqlite()
