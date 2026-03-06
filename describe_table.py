import os
import sys
from database import get_db_cursor

def check_table(table_name):
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute(f"DESCRIBE {table_name}")
            res = cursor.fetchall()
            print(f"Table: {table_name}")
            for r in res:
                print(f"  {r['Field']}: {r['Type']}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_table(sys.argv[1])
    else:
        print("Usage: python describe_table.py <table_name>")
