from database import get_db_cursor
import json

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute('''
        SELECT id, endpoint, error_message, severity, clob_data, created_at 
        FROM sys_transaction_logs 
        WHERE status_code = 404 OR error_message LIKE '%404%' OR failure_mode = 'HTTP_404'
        ORDER BY created_at DESC 
        LIMIT 10
    ''')
    rows = cursor.fetchall()
    if not rows:
        print("No 404 errors found in transaction log.")
    for row in rows:
        print(f"ID: {row['id']} | Endpoint: {row['endpoint']} | Time: {row['created_at']}")
        print(f"Error: {row['error_message']}")
        if row['clob_data']:
            try:
                clob = json.loads(row['clob_data'])
                print(f"  -> Path: {clob.get('request_path')}")
                print(f"  -> Referrer: {clob.get('referrer')}")
                print(f"  -> Full exception: {clob.get('traceback')}")
            except Exception as e:
                print(f"  -> Raw CLOB: {row['clob_data'][:200]}")
        print("-" * 50)
