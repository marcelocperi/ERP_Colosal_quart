import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_mari_data():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre, condicion_pago_id, condicion_mixta_id FROM erp_terceros WHERE id = 3")
        mari = cursor.fetchone()
        print(f"Mari Data: {mari}")
        
        if mari and mari['condicion_pago_id']:
            cursor.execute("SELECT * FROM fin_condiciones_pago WHERE id = %s", (mari['condicion_pago_id'],))
            print(f"Condicion Pago: {cursor.fetchone()}")
            
        cursor.execute("SELECT * FROM erp_terceros_condiciones WHERE tercero_id = 3")
        print(f"Extra Condiciones: {cursor.fetchall()}")

if __name__ == "__main__":
    check_mari_data()
