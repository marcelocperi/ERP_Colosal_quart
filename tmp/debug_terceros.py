import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def debug_terceros():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM erp_terceros LIMIT 1")
        row = cursor.fetchone()
        if row:
            print("Columns in erp_terceros:")
            print(list(row.keys()))
            
        print("\nSearching for Mari...")
        cursor.execute("SELECT * FROM erp_terceros")
        all_rows = cursor.fetchall()
        for r in all_rows:
            # Search in all string columns
            match = False
            for val in r.values():
                if isinstance(val, str) and ('MARI' in val.upper() or 'CHINA' in val.upper()):
                    match = True
                    break
            if match:
                print(f"Match Found: ID={r.get('id')}, Name={r.get('nombre')}, RS={r.get('razon_social')}")

if __name__ == "__main__":
    debug_terceros()
