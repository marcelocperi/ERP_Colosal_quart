import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def list_tables_and_find_client():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("--- ALL TABLES ---")
        for t in tables:
            print(list(t.values())[0])
            
        print("\n--- SEARCHING CLIENT ---")
        # Try different possible table names if vta_clientes fails
        table_names = ['vta_clientes', 'clientes', 'sys_clientes']
        for table in table_names:
            try:
                cursor.execute(f"SELECT id, razon_social FROM {table} WHERE razon_social LIKE '%China Mari%'")
                res = cursor.fetchall()
                if res:
                    print(f"Found in {table}: {res}")
                    return res[0]['id'], table
            except:
                continue
    return None, None

if __name__ == "__main__":
    list_tables_and_find_client()
