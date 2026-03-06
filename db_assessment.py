import mariadb
from database import DB_CONFIG

def get_report():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        tables = ['stk_articulos', 'stk_existencias', 'prestamos']
        
        for table in tables:
            cursor.execute(f"SHOW INDEX FROM {table}")
            indexes = cursor.fetchall()
            print(f"Table {table}:")
            for idx in indexes:
                print(f"  {idx['Key_name']} on {idx['Column_name']}")
                
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    get_report()
