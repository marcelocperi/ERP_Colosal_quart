from database import get_db_cursor
with get_db_cursor(dictionary=True) as check:
    check.execute("SHOW COLUMNS FROM sys_transaction_logs")
    for row in check.fetchall():
        print(f"{row['Field']}: {row['Type']}")
