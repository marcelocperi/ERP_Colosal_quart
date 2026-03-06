
import mariadb
from database import DB_CONFIG

def verify():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("VERIFYING ENTERPRISE 1 DATA:")
    
    tables = ['usuarios', 'prestamos', 'libros']
    for t in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {t} WHERE enterprise_id = 1")
        print(f"{t}: {cursor.fetchone()[0]}")
        
    conn.close()

if __name__ == "__main__":
    verify()
