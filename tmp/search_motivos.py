import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def find_sales():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre, tipo FROM stk_motivos WHERE tipo = 'SALIDA'")
        rows = cursor.fetchall()
        print("--- SALIDAS ---")
        for r in rows:
            print(r)
            
        cursor.execute("SELECT id, nombre, tipo FROM stk_motivos WHERE nombre LIKE '%VENTA%'")
        rows = cursor.fetchall()
        print("--- VENTAS ---")
        for r in rows:
            print(r)

if __name__ == "__main__":
    find_sales()
