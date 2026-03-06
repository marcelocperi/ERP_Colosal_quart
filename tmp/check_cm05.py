import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_cm05():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- Table erp_terceros_cm05 ---")
        try:
            cursor.execute("DESCRIBE erp_terceros_cm05")
            for c in cursor.fetchall():
                print(c)
        except Exception as e:
            print("Error:", e)
            
        print("\n--- Data for Client 3 in erp_terceros_cm05 ---")
        try:
            cursor.execute("SELECT * FROM erp_terceros_cm05 WHERE tercero_id = 3")
            for r in cursor.fetchall():
                print(r)
        except Exception as e:
            print("Error:", e)

if __name__ == "__main__":
    check_cm05()
