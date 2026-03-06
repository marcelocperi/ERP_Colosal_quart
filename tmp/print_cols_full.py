import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def print_columns_full():
    with get_db_cursor(dictionary=True) as cursor:
        tables = [
            'erp_terceros',
            'fin_condiciones_pago', 
            'fin_medios_pago', 
            'fin_condiciones_pago_mixtas', 
            'fin_condiciones_pago_mixtas_detalle',
            'erp_terceros_condiciones'
        ]
        for table in tables:
            cursor.execute(f"DESCRIBE {table}")
            cols = [c['Field'] for c in cursor.fetchall()]
            print(f"--- {table} ---")
            print(cols)

if __name__ == "__main__":
    print_columns_full()
