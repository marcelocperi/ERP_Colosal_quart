
import mariadb
from database import DB_CONFIG

def describe_tables():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    tables = ['stk_motivos', 'stock_motivos']
    for t in tables:
        print(f"\nDESCRIBE {t}")
        try:
            cursor.execute(f"DESCRIBE {t}")
            for row in cursor.fetchall():
                print(f"{row['Field']}: {row['Type']}")
        except Exception as e:
            print(f"Error: {e}")
            
    conn.close()

if __name__ == "__main__":
    describe_tables()
