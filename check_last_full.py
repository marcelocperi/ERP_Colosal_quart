from database import get_db_cursor
import json

def check():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM sys_transaction_logs WHERE status='ERROR' ORDER BY id DESC LIMIT 1")
        r = cursor.fetchone()
        if r:
            print(f"ID: {r['id']}")
            print(f"Endpoint: {r['endpoint']}")
            print(f"Mode: {r['failure_mode']}")
            print(f"Message: {r['error_message']}")
            if r['clob_data']:
                try:
                    data = json.loads(r['clob_data'])
                    print("Trace:")
                    print(data.get('traceback', 'N/A'))
                except:
                    print(f"Clob: {r['clob_data']}")
        else:
            print("No errors found.")

if __name__ == "__main__":
    check()
