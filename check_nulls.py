
import mariadb
from database import DB_CONFIG

def check_nulls():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    tables = ['usuarios', 'prestamos', 'libros']
    for t in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {t} WHERE enterprise_id IS NULL")
        c = cursor.fetchone()[0]
        print(f"Table '{t}' has {c} rows with NULL enterprise_id")
        
    cursor.execute(f"SELECT COUNT(*) FROM usuarios WHERE enterprise_id = 1")
    c1 = cursor.fetchone()[0]
    print(f"Table 'usuarios' has {c1} rows with enterprise_id = 1")

    conn.close()

if __name__ == "__main__":
    check_nulls()
