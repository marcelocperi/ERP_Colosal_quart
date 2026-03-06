import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def list_motivos():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre, tipo FROM stk_motivos ORDER BY id")
        rows = cursor.fetchall()
        print(f"Total rows: {len(rows)}")
        for r in rows:
            print(f"ID: {r['id']:3} | {r['nombre']:30} | {r['tipo']}")

if __name__ == "__main__":
    list_motivos()
