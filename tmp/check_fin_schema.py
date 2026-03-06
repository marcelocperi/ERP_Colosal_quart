import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_conditions_and_medios():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- fin_condiciones_pago schema ---")
        cursor.execute("DESCRIBE fin_condiciones_pago")
        for c in cursor.fetchall():
            print(c)
            
        print("\n--- fin_medios_pago schema ---")
        cursor.execute("DESCRIBE fin_medios_pago")
        for c in cursor.fetchall():
            print(c)
            
        print("\n--- Table list (financial) ---")
        cursor.execute("SHOW TABLES WHERE Tables_in_multi_mcp_db LIKE 'fin_%'")
        for t in cursor.fetchall():
            print(t)

if __name__ == "__main__":
    check_conditions_and_medios()
