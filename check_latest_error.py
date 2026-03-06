from database import get_db_cursor
import json
import os

def check():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT id, endpoint, status, error_message, failure_mode, created_at, clob_data
            FROM sys_transaction_logs 
            ORDER BY id DESC LIMIT 5
        """)
        rows = cursor.fetchall()
        for r in rows:
            print(f"[{r['id']}] {r['status']} | {r['failure_mode']} | {r['endpoint']}")
            print(f"Error: {r['error_message']}")
            print(f"Created: {r['created_at']}")
            if r['clob_data']:
                try:
                    data = json.loads(r['clob_data'])
                    print(f"Traceback: {data.get('traceback', 'N/A')[:1000]}")
                except:
                    print(f"Clob: {str(r['clob_data'])[:500]}")
            print("-" * 50)

if __name__ == "__main__":
    check()
