from database import get_db_cursor
import json
import os

os.environ['MARIADB_NO_POOL_LOG'] = '1' # Try to stop pool logs if any

def check():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT id, endpoint, error_message, clob_data 
            FROM sys_transaction_logs 
            WHERE status='ERROR' 
            ORDER BY id DESC LIMIT 20
        """)
        rows = cursor.fetchall()
        for r in rows:
            print(f"ID: {r['id']}")
            print(f"Endpoint: {r['endpoint']}")
            print(f"Error: {r['error_message']}")
            print("Content: " + str(r['clob_data'])[:500] + "...")
            print("-" * 50)

if __name__ == "__main__":
    check()
