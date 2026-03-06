import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def find_payment_tables():
    with get_db_cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
        print("--- Relevant Tables ---")
        for t in tables:
            if "medio" in t or "condicion" in t or "tercero" in t:
                print(t)

if __name__ == "__main__":
    find_payment_tables()
