from database import get_db_cursor
import json

def check():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT id, endpoint, error_message, created_at 
            FROM sys_transaction_logs 
            WHERE status='ERROR' AND id > 570
            ORDER BY id DESC
        """)
        rows = cursor.fetchall()
        for r in rows:
            print(f"[{r['id']}] {r['endpoint']} | {r['error_message']} | {r['created_at']}")
            print("-" * 50)

if __name__ == "__main__":
    check()
