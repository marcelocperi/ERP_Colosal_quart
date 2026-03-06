import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_client_full():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM erp_terceros WHERE id = 3")
        res = cursor.fetchone()
        print("--- CLIENT 3 FULL DATA ---")
        for k, v in res.items():
            print(f"{k}: {v}")

if __name__ == "__main__":
    check_client_full()
