import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_records():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- fin_condiciones_pago records ---")
        cursor.execute("SELECT * FROM fin_condiciones_pago")
        for r in cursor.fetchall():
            print(r)
            
        print("\n--- fin_medios_pago records ---")
        cursor.execute("SELECT * FROM fin_medios_pago")
        for r in cursor.fetchall():
            print(r)

if __name__ == "__main__":
    check_records()
