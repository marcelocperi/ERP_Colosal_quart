from database import get_db_cursor
try:
    with get_db_cursor() as cursor:
        cursor.execute("SELECT 1 WHERE 1 = ?", (1,))
        print("SQL with '?' works!")
except Exception as e:
    print(f"SQL with '?' failed: {e}")

try:
    with get_db_cursor() as cursor:
        cursor.execute("SELECT 1 WHERE 1 = %s", (1,))
        print("SQL with '%s' works!")
except Exception as e:
    print(f"SQL with '%s' failed: {e}")
