import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def check_nc():
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Get NC codes
        cursor.execute("SELECT codigo, descripcion FROM sys_tipos_comprobante WHERE descripcion LIKE '%Nota de Cr%dito%'")
        nc_types = cursor.fetchall()
        print('Tipos de comprobante de Nota de Credito:')
        for t in nc_types:
            print(f"Code: {t['codigo']}, Desc: {t['descripcion']}")
        
        if not nc_types:
            print("No NC types found.")
            return

        codes = [t['codigo'] for t in nc_types]
        
        # 2. Query comprobantes for enterprise 0
        sql = f"""
            SELECT c.id, c.tipo_operacion, tc.descripcion, tc.letra, c.punto_venta, c.numero, c.importe_total, t.nombre as tercero, c.modulo
            FROM erp_comprobantes c
            JOIN sys_tipos_comprobante tc ON c.tipo_comprobante = tc.codigo
            JOIN erp_terceros t ON c.tercero_id = t.id
            WHERE c.enterprise_id = 0 
              AND c.tipo_comprobante IN ({','.join(['%s']*len(codes))})
        """
        cursor.execute(sql, tuple(codes))
        results = cursor.fetchall()
        print(f"\nNotas de Credito encontradas para Empresa 0: {len(results)}")
        for r in results:
            print(r)

if __name__ == '__main__':
    check_nc()
