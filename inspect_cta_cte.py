import sys
sys.path.insert(0, '.')
from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    for table in ['fin_recibos', 'fin_recibos_detalles', 'fin_recibos_medios', 'fin_factura_cobros']:
        cursor.execute(f'DESCRIBE {table}')
        cols = cursor.fetchall()
        print(f'\n=== {table} ===')
        for c in cols:
            print(f"  {c['Field']} - {c['Type']}")
