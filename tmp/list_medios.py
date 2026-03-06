import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def list_medios():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- Columns of fin_medios_pago ---")
        cursor.execute("DESCRIBE fin_medios_pago")
        cols = [c['Field'] for c in cursor.fetchall()]
        print(cols)
        
        print("\n--- Rows of fin_medios_pago ---")
        cursor.execute("SELECT id, nombre, tipo FROM fin_medios_pago WHERE activo = 1")
        for r in cursor.fetchall():
            print(r)
            
        print("\n--- Tables starting with erp_terceros_ ---")
        cursor.execute("SHOW TABLES LIKE 'erp_terceros_%'")
        for t in cursor.fetchall():
            print(t)

if __name__ == "__main__":
    list_medios()
