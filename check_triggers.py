
from database import get_db_cursor

def check_triggers():
    with get_db_cursor() as cursor:
        cursor.execute("SHOW TRIGGERS")
        rows = cursor.fetchall()
        print("TRIGGERS:")
        for row in rows:
            print(row)

if __name__ == "__main__":
    check_triggers()
