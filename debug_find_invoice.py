import sys
sys.path.append('multiMCP')
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    print("--- ENTERPRISES ---")
    cursor.execute('SELECT id, nombre, condicion_iva FROM sys_enterprises')
    for r in cursor.fetchall():
        print(r)
    
    print("\n--- INVOICES NRO 2 ---")
    cursor.execute("SELECT id, enterprise_id, tipo_comprobante, numero, importe_total FROM erp_comprobantes WHERE numero = 2")
    for r in cursor.fetchall():
        print(r)
