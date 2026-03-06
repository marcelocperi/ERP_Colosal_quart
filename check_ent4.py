
import mariadb
from database import DB_CONFIG

def check_ent4():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM usuarios WHERE enterprise_id = 4")
    print(f"Users in Ent 4: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM prestamos WHERE enterprise_id = 4")
    print(f"Loans in Ent 4: {cursor.fetchone()[0]}")
    
    conn.close()

if __name__ == "__main__":
    check_ent4()
