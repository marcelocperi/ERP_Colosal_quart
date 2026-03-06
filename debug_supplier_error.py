from database import get_db_cursor
import json

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT * FROM sys_transaction_logs WHERE endpoint LIKE '%/compras/proveedores/perfil/%' ORDER BY created_at DESC LIMIT 5")
    rows = cursor.fetchall()
    for row in rows:
        print("-" * 50)
        print(f"ID: {row.get('id')} | Date: {row.get('created_at')}")
        print(f"Endpoint: {row.get('endpoint')} | Method: {row.get('request_method')}")
        print(f"Error: {row.get('error_message')}")
        
        tb = row.get('error_traceback') or row.get('clob_data')
        if tb:
            print(f"Traceback: {tb}")
        else:
            print("No traceback found.")
