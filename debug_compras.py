import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def check_data():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT c.id, c.modulo, c.tipo_comprobante, c.numero, t.nombre, t.es_cliente, t.es_proveedor 
            FROM erp_comprobantes c 
            JOIN erp_terceros t ON c.tercero_id = t.id 
            WHERE c.modulo = 'COMPRAS'
        """)
        rows = cursor.fetchall()
        print(f"Found {len(rows)} purchase documents:")
        for row in rows:
            print(row)

if __name__ == '__main__':
    check_data()
