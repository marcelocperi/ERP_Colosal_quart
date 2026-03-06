from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT id, created_at, level, action, details, error_traceback FROM sys_transaction_logs ORDER BY id DESC LIMIT 5")
    rows = cursor.fetchall()
    if not rows:
        print("No hay logs recientes en sys_transaction_logs.")
    else:
        for r in rows:
            print(f"[{r['created_at']}] {r['level']} | {r['action']} | {r['details']}")
            print(f"TRACE: {str(r['error_traceback'])[:300]}")
            print("-" * 50)
