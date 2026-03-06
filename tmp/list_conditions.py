import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def list_all_conditions():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- ALL fin_condiciones_pago ---")
        cursor.execute("SELECT id, nombre, dias_vencimiento FROM fin_condiciones_pago ORDER BY id")
        for r in cursor.fetchall():
            print(f"ID: {r['id']:2} | {r['nombre']:20} | {r['dias_vencimiento']} days")

if __name__ == "__main__":
    list_all_conditions()
