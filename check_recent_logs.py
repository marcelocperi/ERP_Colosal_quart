from database import get_db_cursor
import json

def check_logs():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM sys_transaction_logs WHERE status = 'ERROR' ORDER BY created_at DESC LIMIT 10")
        logs = cursor.fetchall()
        for log in logs:
            print(f"ID: {log['id']} | Created: {log['created_at']}")
            print(f"Endpoint: {log['endpoint']}")
            print(f"Error Message: {log['error_message']}")
            # print(f"Traceback: {log['error_traceback'][:500] if log['error_traceback'] else 'N/A'}")
            print("-" * 50)

if __name__ == "__main__":
    check_logs()
