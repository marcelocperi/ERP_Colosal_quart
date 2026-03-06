
import mariadb
from database import DB_CONFIG

def debug_counts():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("\nCOUNTS PER ENTERPRISE:")
        
        print("\n--- Patrons (usuarios) ---")
        cursor.execute("SELECT enterprise_id, COUNT(*) FROM usuarios GROUP BY enterprise_id")
        for ent, count in cursor.fetchall():
            print(f"Enterprise {ent}: {count} patrons")
            
        print("\n--- Loans (prestamos) ---")
        cursor.execute("SELECT enterprise_id, COUNT(*) FROM prestamos GROUP BY enterprise_id")
        for ent, count in cursor.fetchall():
            print(f"Enterprise {ent}: {count} loans")
            
        print("\n--- System Users (sys_users) ---")
        cursor.execute("SELECT id, username, enterprise_id FROM sys_users WHERE id=1")
        row = cursor.fetchone()
        print(f"Admin User (ID 1) is in Enterprise: {row[2]}")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_counts()
