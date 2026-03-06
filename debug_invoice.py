from database import get_db_cursor
import json
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

with get_db_cursor(dictionary=True) as cursor:
    # 1. Buscar el cliente
    cursor.execute("SELECT id, nombre, enterprise_id FROM erp_terceros WHERE nombre LIKE '%GLOBAL TEST PERCEPCIONES%'")
    clientes = cursor.fetchall()
    print("=== CLIENTES ENCONTRADOS ===")
    print(json.dumps(clientes, indent=4, cls=DecimalEncoder))
    
    if clientes:
        cliente_id = clientes[0]['id']
        ent_id = clientes[0]['enterprise_id']
        
        # 2. Buscar comprobantes
        cursor.execute("SELECT id, tipo_comprobante, numero, importe_total, modulo, enterprise_id FROM erp_comprobantes WHERE tercero_id = %s", (cliente_id,))
        comprobantes = cursor.fetchall()
        print("\n=== COMPROBANTES DEL CLIENTE ===")
        print(json.dumps(comprobantes, indent=4, cls=DecimalEncoder))
        
        # 3. Revisar el modulo de la consulta en la cuenta corriente
        cursor.execute("SELECT count(*) as total FROM erp_comprobantes WHERE tercero_id = %s AND modulo = 'VEN'", (cliente_id, ))
        print("\n=== TOTAL COMPROBANTES CON MODULO 'VEN' (Global) ===")
        print(cursor.fetchone())

        cursor.execute("SELECT count(*) as total FROM erp_comprobantes WHERE tercero_id = %s AND enterprise_id = %s AND modulo = 'VEN'", (cliente_id, ent_id))
        print("\n=== TOTAL COMPROBANTES CON MODULO 'VEN' (Específico al Cliente) ===")
        print(cursor.fetchone())
