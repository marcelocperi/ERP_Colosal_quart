import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def print_columns():
    with get_db_cursor(dictionary=True) as cursor:
        for table in ['fin_condiciones_pago', 'fin_medios_pago', 'fin_condiciones_pago_mixtas', 'fin_condiciones_pago_mixtas_detalle']:
            cursor.execute(f"DESCRIBE {table}")
            print(f"--- {table} ---")
            print([c['Field'] for c in cursor.fetchall()])

if __name__ == "__main__":
    print_columns()
