import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_mari_data():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- erp_terceros Data for Mari (ID 3) ---")
        cursor.execute("SELECT id, nombre, condicion_pago_id, condicion_mixta_id FROM erp_terceros WHERE id = 3")
        mari = cursor.fetchone()
        print(mari)
        
        if mari:
            cp_id = mari['condicion_pago_id']
            if cp_id:
                print(f"\n--- Payment Condition (ID {cp_id}) ---")
                cursor.execute("SELECT * FROM fin_condiciones_pago WHERE id = %s", (cp_id,))
                print(cursor.fetchone())
            
            # Check erp_terceros_condiciones
            print("\n--- erp_terceros_condiciones for Mari ---")
            cursor.execute("SELECT * FROM erp_terceros_condiciones WHERE tercero_id = 3")
            print(cursor.fetchall())

if __name__ == "__main__":
    check_mari_data()
