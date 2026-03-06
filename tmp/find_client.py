import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def find_china_mari():
    with get_db_cursor(dictionary=True) as cursor:
        for table in ['clientes', 'erp_terceros']:
            print(f"Searching in {table}...")
            cursor.execute(f"SELECT * FROM {table} WHERE razon_social LIKE '%MARI%' OR razon_social LIKE '%CHINA%'")
            results = cursor.fetchall()
            if results:
                print(f"Found {len(results)} results in {table}:")
                for r in results:
                    print(f"ID: {r.get('id')}, Name: {r.get('razon_social')}")
                return results[0]['id'], table
    return None, None

if __name__ == "__main__":
    find_china_mari()
