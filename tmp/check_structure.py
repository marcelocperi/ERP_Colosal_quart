import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_structure():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- erp_terceros columns ---")
        cursor.execute("DESCRIBE erp_terceros")
        print([c['Field'] for c in cursor.fetchall()])
        
        print("\n--- fin_medios_pago columns ---")
        cursor.execute("DESCRIBE fin_medios_pago")
        print([c['Field'] for c in cursor.fetchall()])
        
        print("\n--- fin_condiciones_pago columns ---")
        cursor.execute("DESCRIBE fin_condiciones_pago")
        print([c['Field'] for c in cursor.fetchall()])
        
        print("\n--- fin_condiciones_pago_mixtas_detalle columns ---")
        cursor.execute("DESCRIBE fin_condiciones_pago_mixtas_detalle")
        print([c['Field'] for c in cursor.fetchall()])

if __name__ == "__main__":
    check_structure()
