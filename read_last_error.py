import json
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT error_message, error_traceback FROM sys_transaction_logs WHERE status='ERROR' ORDER BY created_at DESC LIMIT 2")
    for row in cursor.fetchall():
        print('='*50)
        print('MSG:', row['error_message'])
        try:
            data = json.loads(row['error_traceback'] or '{}')
            print(data.get('traceback', row['error_traceback']))
        except:
            print(row['error_traceback'])
