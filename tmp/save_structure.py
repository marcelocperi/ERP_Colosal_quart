import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def save_structure():
    with get_db_cursor(dictionary=True) as cursor:
        with open("tmp_structure_full.txt", "w") as f:
            for table in ['erp_terceros', 'fin_medios_pago', 'fin_condiciones_pago', 'fin_condiciones_pago_mixtas_detalle', 'erp_terceros_condiciones', 'fin_condiciones_pago_mixtas']:
                cursor.execute(f"DESCRIBE {table}")
                cols = [c['Field'] for c in cursor.fetchall()]
                f.write(f"--- {table} ---\n")
                f.write(", ".join(cols) + "\n\n")

if __name__ == "__main__":
    save_structure()
