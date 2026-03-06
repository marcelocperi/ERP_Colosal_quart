import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_client_rules():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- RULES FOR CLIENT ID 3 ---")
        cursor.execute("SELECT id, nombre, condicion_pago_id FROM erp_terceros WHERE id = 3")
        print(cursor.fetchone())
        
        cursor.execute("""
            SELECT tc.*, cp.nombre as condicion_nombre 
            FROM erp_terceros_condiciones tc
            JOIN fin_condiciones_pago cp ON tc.condicion_pago_id = cp.id
            WHERE tc.tercero_id = 3
        """)
        print("Detailed conditions:", cursor.fetchall())

if __name__ == "__main__":
    check_client_rules()
