from database import get_db_cursor
import sys

def check_columns(table_name):
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute(f"DESC {table_name}")
            for row in cursor.fetchall():
                print(row['Field'])
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_columns(sys.argv[1])
    else:
        check_columns("cmp_items_cotizacion")
