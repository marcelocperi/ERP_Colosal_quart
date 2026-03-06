import mariadb
from database import DB_CONFIG

def dump_schema():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        tables = ['stk_articulos', 'stk_tipos_articulo']
        
        for table in tables:
            print(f"\n--- Schema for {table} ---")
            cursor.execute(f"DESCRIBE {table}")
            for row in cursor.fetchall():
                print(row)
                
        print("\n--- Data for stk_tipos_articulo ---")
        cursor.execute("SELECT * FROM stk_tipos_articulo")
        for row in cursor.fetchall():
            print(row)
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    dump_schema()
