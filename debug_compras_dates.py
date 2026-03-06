import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def check_dates():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT id, fecha_emision, importe_total 
            FROM erp_comprobantes 
            WHERE modulo = 'COMPRAS' 
            ORDER BY fecha_emision DESC 
        """)
        rows = cursor.fetchall()
        print(f"Checking {len(rows)} purchase docs:")
        for row in rows:
            print(row)

if __name__ == '__main__':
    check_dates()
