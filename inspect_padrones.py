
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_db_cursor

def inspect():
    print("Inspect sys_padrones_iibb table...")
    try:
        with get_db_cursor() as cursor:
            cursor.execute("DESCRIBE sys_padrones_iibb")
            rows = cursor.fetchall()
            for r in rows:
                print(r)
    except Exception as e:
        print(f"Error inspecting table: {e}")

if __name__ == "__main__":
    inspect()
