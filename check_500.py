from database import get_db_cursor
import json
import os

def check():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT id, endpoint, failure_mode, error_message, clob_data 
            FROM sys_transaction_logs 
            WHERE failure_mode='UNHANDLED_EXCEPTION' OR failure_mode LIKE 'HTTP_5%'
             ORDER BY id DESC LIMIT 10
        """)
        rows = cursor.fetchall()
        for r in rows:
            print(f"ID: {r['id']}")
            print(f"Endpoint: {r['endpoint']}")
            print(f"Mode: {r['failure_mode']}")
            print(f"Error: {r['error_message']}")
            print("Trace: " + str(r['clob_data'])[:500] + "...")
            print("-" * 50)

if __name__ == "__main__":
    check()
