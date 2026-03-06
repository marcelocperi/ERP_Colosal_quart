
import mariadb
from database import DB_CONFIG

def debug_db():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("\n--- CHECKING 'usuarios' TABLE (Patrons) ---")
        cursor.execute("SELECT id, nombre, apellido, enterprise_id FROM usuarios LIMIT 10")
        rows = cursor.fetchall()
        print(f"Total rows found: {len(rows)}")
        for r in rows:
            print(r)
            
        print("\n--- CHECKING 'prestamos' TABLE (Loans) ---")
        cursor.execute("SELECT id, usuario_id, libro_id, enterprise_id FROM prestamos LIMIT 10")
        rows = cursor.fetchall()
        print(f"Total rows found: {len(rows)}")
        for r in rows:
            print(r)
            
        print("\n--- CHECKING 'sys_users' TABLE (System Users) ---")
        cursor.execute("SELECT id, username, enterprise_id FROM sys_users LIMIT 10")
        rows = cursor.fetchall()
        for r in rows:
            print(r)

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_db()
