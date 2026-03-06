import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_conditions_schema():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- erp_terceros_condiciones ---")
        try:
            cursor.execute("DESCRIBE erp_terceros_condiciones")
            for r in cursor.fetchall():
                print(f"{r['Field']} ({r['Type']})")
        except Exception as e:
            print(f"Error: {e}")
            
        print("\n--- erp_terceros_cm05 ---")
        try:
            cursor.execute("DESCRIBE erp_terceros_cm05")
            for r in cursor.fetchall():
                print(f"{r['Field']} ({r['Type']})")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    check_conditions_schema()
