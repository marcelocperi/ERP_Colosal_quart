import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_overlap():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- Conditions ---")
        cursor.execute("SELECT id, nombre FROM fin_condiciones_pago")
        conds = cursor.fetchall()
        for c in conds:
            print(c)
            
        print("\n--- Media ---")
        cursor.execute("SELECT id, nombre, tipo FROM fin_medios_pago")
        medios = cursor.fetchall()
        for m in medios:
            print(m)

if __name__ == "__main__":
    check_overlap()
