import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def final_check_0():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT id, tipo_comprobante, punto_venta, numero, importe_total, modulo, tipo_operacion
            FROM erp_comprobantes
            WHERE enterprise_id = 0
        """)
        results = cursor.fetchall()
        print(f"Total documentos encontrados para Empresa 0: {len(results)}")
        for r in results:
            # Let's see if any code matches NC patterns (usually 003, 008, 013, 011...)
            print(r)

if __name__ == '__main__':
    final_check_0()
