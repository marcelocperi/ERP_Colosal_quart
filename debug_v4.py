
import mariadb
from database import DB_CONFIG

def debug_db():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("\n=== SYSTEM USERS (sys_users) ===")
        cursor.execute("SELECT id, username, enterprise_id FROM sys_users")
        for r in cursor.fetchall():
            print(f"ID={r[0]} | User={r[1]} | Ent={r[2]}")

        print("\n=== LIBRARY USERS/PATRONS (usuarios) ===")
        cursor.execute("SELECT id, nombre, apellido, enterprise_id FROM usuarios")
        rows = cursor.fetchall()
        if not rows:
            print("NO PATRONS FOUND IN 'usuarios' TABLE!")
        for r in rows:
            print(f"ID={r[0]} | Name={r[1]} {r[2]} | Ent={r[3]}")
    
        print("\n=== LOANS (prestamos) ===")
        cursor.execute("SELECT id, usuario_id, libro_id, enterprise_id FROM prestamos")
        rows = cursor.fetchall()
        if not rows:
            print("NO LOANS FOUND IN 'prestamos' TABLE!")
        for r in rows:
            print(f"LoanID={r[0]} | UserID={r[1]} | BookID={r[2]} | Ent={r[3]}")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_db()
