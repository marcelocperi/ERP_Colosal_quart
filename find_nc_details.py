import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def find_nc_details():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT e.nombre as empresa, c.enterprise_id, tc.descripcion, tc.letra, c.punto_venta, c.numero, c.importe_total, t.nombre as tercero, c.tipo_operacion
            FROM erp_comprobantes c
            JOIN sys_enterprises e ON c.enterprise_id = e.id
            JOIN sys_tipos_comprobante tc ON c.tipo_comprobante = tc.codigo
            JOIN erp_terceros t ON c.tercero_id = t.id
            WHERE tc.descripcion LIKE '%Nota de Cr%dito%'
        """)
        results = cursor.fetchall()
        for r in results:
            print(r)

if __name__ == '__main__':
    find_nc_details()
