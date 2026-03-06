
from database import get_db_cursor
import sys

def get_schema():
    with get_db_cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
        target = [t for t in tables if 'incident' in t.lower()]
        print(f"Tables matching 'incident': {target}")
        for t in target:
            print(f"\n--- {t} ---")
            cursor.execute(f"DESC {t}")
            for row in cursor.fetchall():
                print(row)

if __name__ == "__main__":
    get_schema()
