from database import get_db_cursor
import json

def check_schema():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("DESCRIBE cmp_ordenes_compra")
        columns = cursor.fetchall()
        for col in columns:
            print(col['Field'])

if __name__ == "__main__":
    check_schema()
