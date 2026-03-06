
import mariadb
from database import DB_CONFIG

def check_tables():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        tables_to_check = ['stock_ajustes', 'stk_movimientos', 'stk_movimientos_detalle', 'stk_existencias', 'stk_motivos', 'movimientos_pendientes']
        

        for table in tables_to_check:
            cursor.execute(f"SHOW TABLES LIKE '{table}'")
            res = cursor.fetchone()
            if res:
                print(f"[FOUND] Table '{table}' exists.")
            else:
                print(f"[MISSING] Table '{table}' DOES NOT exist.")
                
        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_tables()
