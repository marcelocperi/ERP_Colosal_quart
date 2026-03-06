import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def save_structure_v2():
    with get_db_cursor(dictionary=True) as cursor:
        with open("tmp_structure_full_v2.txt", "w") as f:
            tables = [
                'erp_terceros_cm05',
                'fin_recibos_medios',
                'fin_ordenes_pago_medios'
            ]
            for table in tables:
                try:
                    cursor.execute(f"DESCRIBE {table}")
                    cols = [c['Field'] for c in cursor.fetchall()]
                    f.write(f"--- {table} ---\n")
                    f.write(", ".join(cols) + "\n\n")
                except:
                    f.write(f"--- {table} (NOT FOUND) ---\n\n")

if __name__ == "__main__":
    save_structure_v2()
