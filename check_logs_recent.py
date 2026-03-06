from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT id, created_at, status, endpoint, error_message FROM sys_transaction_logs ORDER BY id DESC LIMIT 10")
    for r in cursor.fetchall():
        print(f"[{r['id']}] [{r['created_at']}] [{r['status']}] {r['endpoint']} - {r['error_message']}")
