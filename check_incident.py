from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("""
        SELECT * FROM sys_transaction_logs 
        WHERE status = 'ERROR' 
        ORDER BY created_at DESC 
        LIMIT 1
    """)
    incident = cursor.fetchone()
    if incident:
        print(f"Incident ID: {incident['id']}")
        print(f"Endpoint: {incident['endpoint']}")
        print(f"Error Message: {incident['error_message']}")
        print(f"Traceback: {incident['error_traceback']}")
    else:
        print("No recent error incidents found.")
