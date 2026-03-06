import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def check_all_empresa0():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT tc.descripcion, COUNT(*) as cantidad
            FROM erp_comprobantes c
            JOIN sys_tipos_comprobante tc ON c.tipo_comprobante = tc.codigo
            WHERE c.enterprise_id = 0
            GROUP BY tc.descripcion
        """)
        results = cursor.fetchall()
        print("Tipos de comprobantes existentes para Empresa 0:")
        for r in results:
            print(r)

if __name__ == '__main__':
    check_all_empresa0()
