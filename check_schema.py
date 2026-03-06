import os
import sys

# Ensure we can import from the current directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import get_db_cursor

def check_tables():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("DESCRIBE erp_comprobantes_detalle")
        res = cursor.fetchall()
        print([r['Field'] for r in res])

if __name__ == "__main__":
    check_tables()
