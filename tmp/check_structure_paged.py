import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def print_paged(label, items):
    print(f"\n--- {label} ---")
    for i in range(0, len(items), 10):
        print(", ".join(items[i:i+10]))

def check_structure_paged():
    with get_db_cursor(dictionary=True) as cursor:
        for table in ['erp_terceros', 'fin_medios_pago', 'fin_condiciones_pago', 'fin_condiciones_pago_mixtas_detalle', 'erp_terceros_condiciones']:
            cursor.execute(f"DESCRIBE {table}")
            cols = [c['Field'] for c in cursor.fetchall()]
            print_paged(table, cols)

if __name__ == "__main__":
    check_structure_paged()
