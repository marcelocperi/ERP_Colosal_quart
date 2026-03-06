import os
import mariadb
from database import DB_CONFIG

def apply_stock_schema():
    print("Iniciando aplicación de esquema de STOCK en MariaDB...")
    
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        with open('stk_schema.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Adaptar sintaxis SQLite a MariaDB de forma más limpia
        # Primero quitamos los comentarios para evitar problemas con split
        clean_lines = []
        for line in sql_content.splitlines():
            if not line.strip().startswith('--'):
                clean_lines.append(line)
        
        clean_sql = "\n".join(clean_lines)
        
        # Reemplazos específicos
        clean_sql = clean_sql.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'INT PRIMARY KEY AUTO_INCREMENT')
        clean_sql = clean_sql.replace('INSERT OR IGNORE', 'INSERT IGNORE')
        
        # MariaDB prefiere DATETIME sin precision opcional en DEFAULT si ya es compatible
        # Pero DEFAULT CURRENT_TIMESTAMP es estándar.
        
        commands = clean_sql.split(';')
        
        for command in commands:
            cmd = command.strip()
            if cmd:
                try:
                    cursor.execute(cmd)
                    # print(f"OK: {cmd[:40].replace('\n', ' ')}...")
                except mariadb.Error as e:
                    print(f"❌ Error en comando:\n{cmd}\nError: {e}")
        
        conn.commit()
        print("✅ Proceso de aplicación finalizado.")
        
    except mariadb.Error as e:
        print(f"❌ Error crítico de conexión: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    apply_stock_schema()
