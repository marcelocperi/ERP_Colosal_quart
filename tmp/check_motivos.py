import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_motivos():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- Valid stk_motivos ---")
        cursor.execute("SELECT id, nombre, tipo FROM stk_motivos")
        rows = cursor.fetchall()
        for r in rows:
            print(r)

if __name__ == "__main__":
    check_motivos()
