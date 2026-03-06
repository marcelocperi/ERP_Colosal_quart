import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def search_client():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, razon_social FROM vta_clientes WHERE razon_social LIKE '%China Mari%'")
        clients = cursor.fetchall()
        print(f"Clients found: {clients}")
        
        if clients:
            client_id = clients[0]['id']
            # Search for tables that might link clients to payment methods
            cursor.execute("SHOW TABLES LIKE '%vta_clientes%'")
            tables = cursor.fetchall()
            print(f"Client related tables: {tables}")
            
            cursor.execute("SHOW TABLES LIKE '%pago%'")
            payment_tables = cursor.fetchall()
            print(f"Payment related tables: {payment_tables}")

if __name__ == "__main__":
    search_client()
