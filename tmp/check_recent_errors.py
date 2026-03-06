from database import get_db_cursor
import json

def check_recent_errors():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM sys_transaction_logs ORDER BY created_at DESC LIMIT 5")
        errors = cursor.fetchall()
        for err in errors:
            print(f"Time: {err['created_at']}")
            print(f"Module: {err['module']}")
            print(f"Endpoint: {err['endpoint']}")
            print(f"Message: {err['error_message']}")
            print("-" * 20)

if __name__ == "__main__":
    check_recent_errors()
