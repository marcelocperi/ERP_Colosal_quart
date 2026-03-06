import os
import sys

# Attempt to import a database driver
try:
    import mariadb as db_driver
    print("Usando driver: mariadb")
except ImportError:
    try:
        import pymysql as db_driver
        print("Usando driver: pymysql")
    except ImportError:
        try:
            import mysql.connector as db_driver
            print("Usando driver: mysql.connector")
        except ImportError:
            print("ERROR: No se encontró ningún driver de base de datos (mariadb, pymysql, mysql-connector).")
            print("Ejecuta: pip install pymysql")
            sys.exit(1)

def run_setup():
    print("="*50)
    print("ASISTENTE DE CONFIGURACIÓN DE BASE DE DATOS (CLOUD)")
    print("="*50)
    print("Este script inicializará la base de datos leyendo 'erp_schema.sql'.")
    print("Asegúrate de haber creado la base de datos en el panel de control primero.")
    print("-" * 50)

    # 1. Get Credentials
    db_host = os.environ.get('DB_HOST') or input("Ingrese DB Host (ej: usuario.mysql.pythonanywhere-services.com): ").strip()
    db_user = os.environ.get('DB_USER') or input("Ingrese DB User (ej: usuario): ").strip()
    db_password = os.environ.get('DB_PASSWORD') or input("Ingrese DB Password: ").strip()
    db_name = os.environ.get('DB_NAME') or input("Ingrese DB Name (ej: usuario$multi_mcp_db): ").strip()
    db_port = int(os.environ.get('DB_PORT', 3306))

    config = {
        'user': db_user,
        'password': db_password,
        'host': db_host,
        'port': db_port,
        'database': db_name
    }
    
    # 2. Connect
    print(f"\nConectando a {db_host}...")
    try:
        conn = db_driver.connect(**config)
        cursor = conn.cursor()
        print("¡Conexión Exitosa!")
    except Exception as e:
        print(f"ERROR DE CONEXIÓN: {e}")
        print("Verifique host, usuario y contraseña.")
        return

    # 3. Read SQL
    print("\nLeyendo erp_schema.sql...")
    try:
        with open('erp_schema.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
    except FileNotFoundError:
        print("ERROR: No se encuentra el archivo 'erp_schema.sql' en esta carpeta.")
        return

    # 4. Execute
    # Split by semicolon, but be careful with stored procedures if any. 
    # Provided schema seems simple enough for splitting by ';' provided no ';' in strings/comments roughly.
    # Ideally use a robust parser, but for generic dump 'sql_commands = sql_content.split(';')' is a basic heuristic.
    # Better: execute script directly if driver supports it, or line by line.
    
    # Simple split (prone to errors if ; in text, but good enough for structure)
    commands = sql_content.split(';')
    
    print(f"Ejecutando {len(commands)} sentencias SQL...")
    
    count_success = 0
    count_error = 0
    
    for cmd in commands:
        cmd = cmd.strip()
        if not cmd: continue
        try:
            cursor.execute(cmd)
            count_success += 1
        except Exception as e:
            # Ignore some errors like "table exists" if simple
            print(f"Advertencia en comando: {cmd[:50]}... -> {e}")
            count_error += 1
            
    try:
        # Create initial admin user if not exists
        print("\nVerificando usuario Admin inicial...")
        cursor.execute("SELECT id FROM sys_roles WHERE id=1")
        if not cursor.fetchone():
             cursor.execute("INSERT INTO sys_enterprises (id, nombre, estado) VALUES (1, 'Mi Empresa', 'activo')")
             cursor.execute("INSERT INTO sys_roles (id, enterprise_id, name) VALUES (1, 1, 'AdminSys')")
             print(" -> Empresa y Rol Admin creados.")
        
        # Check Admin User
        cursor.execute("SELECT id FROM sys_users WHERE username='admin'")
        if not cursor.fetchone():
             # Create default admin: admin / Admin123
             # Hash: pbkdf2:sha256:600000$y... (Example, but we need to generate it or ask user)
             # Let's generate a hardcoded generic hash for 'Admin123' to keep it simple without werkzeug dependency if possible,
             # BUT we likely have werkzeug installed.
             from werkzeug.security import generate_password_hash
             pw_hash = generate_password_hash("Admin123")
             cursor.execute("INSERT INTO sys_users (enterprise_id, username, password_hash, role_id) VALUES (1, 'admin', ?, 1)", (pw_hash,))
             print(" -> Usuario 'admin' creado con contraseña 'Admin123'.")
        else:
             print(" -> Usuario 'admin' ya existe.")
             
        conn.commit()
        print("\n" + "="*50)
        print("INSTALACIÓN COMPLETADA")
        print(f"Sentencias exitosas: {count_success}")
        print(f"Errores (ignorados): {count_error}")
        print("="*50)
        
    except Exception as e:
        print(f"Error en post-configuracion: {e}")
        conn.rollback()
    
    conn.close()

if __name__ == "__main__":
    run_setup()
