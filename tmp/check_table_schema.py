from database import get_db_cursor
import json

def check_schema(table):
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute(f"DESCRIBE {table}")
        columns = cursor.fetchall()
        for col in columns:
            print(col['Field'])

if __name__ == "__main__":
    import sys
    table = sys.argv[1] if len(sys.argv) > 1 else 'cmp_ordenes_compra'
    check_schema(table)
