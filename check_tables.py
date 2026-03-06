
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_db_cursor

def check():
    print("Checking tables...")
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'tax_engine%'")
            rows = cursor.fetchall()
            for r in rows:
                print(r)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check()
