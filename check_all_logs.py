from database import get_db_cursor
import json
import os

def check():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT id, endpoint, status, error_message, failure_mode, created_at 
            FROM sys_transaction_logs 
            ORDER BY id DESC LIMIT 20
        """)
        rows = cursor.fetchall()
        for r in rows:
            print(f"[{r['id']}] {r['status']} | {r['failure_mode']} | {r['endpoint']} | {r['error_message']} | {r['created_at']}")
            print("-" * 50)

if __name__ == "__main__":
    check()
