import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_schema():
    with get_db_cursor(dictionary=True) as cursor:
        for table in ['clientes', 'erp_terceros']:
            print(f"--- Schema for {table} ---")
            try:
                cursor.execute(f"DESCRIBE {table}")
                cols = cursor.fetchall()
                for c in cols:
                    print(f"{c['Field']} ({c['Type']})")
            except Exception as e:
                print(f"Error describing {table}: {e}")

if __name__ == "__main__":
    check_schema()
