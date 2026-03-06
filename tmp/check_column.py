from database import get_db_cursor
import sys

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'status'")
    print(cursor.fetchone())
