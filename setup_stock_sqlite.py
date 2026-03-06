import sqlite3
import os

DB_NAME = os.environ.get("DB_NAME", "multi_mcp.db")

def setup_stock():
    print(f"Cargando Módulo de Stock Stock en {DB_NAME}")
    
    if 'HOME' in os.environ and '/home/' in os.environ['HOME']:
        db_path = os.path.join(os.environ['HOME'], 'multiMCP', DB_NAME)
    else:
        db_path = DB_NAME
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Read Stock Schema
    print("Ejecutando stk_schema.sql...")
    try:
        with open('stk_schema.sql', 'r', encoding='utf-8') as f:
            content = f.read()
            # Simple parsing: split by semicolon
            commands = content.split(';')
            for cmd in commands:
                cmd = cmd.strip()
                if not cmd: continue
                # Basic check for comments
                if cmd.startswith('--'): continue
                
                try:
                    cursor.execute(cmd)
                except sqlite3.OperationalError as e:
                    if "already exists" in str(e):
                        pass
                    else:
                        print(f"Error ejecutar SQL Stock: {e}")
                        # print(cmd[:50])
                        
        print("Tablas de Stock creadas.")
        
        # 2. SEED DATA (Force insert if needed because 'ignore' might fail on some sqlite versions if table created just now)
        # The SQL file has INSERT OR IGNORE statements which handle this.
        
        conn.commit()
    except Exception as e:
        print(f"Error cargando stock schema: {e}")
        
    conn.close()
    print("¡Módulo de Stock Inicializado!")

if __name__ == "__main__":
    setup_stock()
