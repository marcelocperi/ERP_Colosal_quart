
from database import get_db_cursor
import sys

def find_table():
    with get_db_cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
        matches = [t for t in tables if 'incident' in t.lower() or 'log' in t.lower() or 'issue' in t.lower()]
        for m in matches:
            print(f"Table found: {m}")
            cursor.execute(f"DESC {m}")
            cols = [c[0] for c in cursor.fetchall()]
            print(f"  Columns: {cols}")

if __name__ == "__main__":
    find_table()
