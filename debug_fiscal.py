from database import get_db_cursor
import json
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT * FROM sys_jurisdicciones LIMIT 5")
    juris = cursor.fetchall()
    print("JURIS:" + json.dumps(juris, default=decimal_default))
    
    cursor.execute("SELECT id, nombre, cuit FROM erp_terceros WHERE es_cliente=1 LIMIT 5")
    clients = cursor.fetchall()
    print("CLIENTS:" + json.dumps(clients, default=decimal_default))
    
    if clients:
        c_id = clients[0]['id']
        cursor.execute("SELECT * FROM erp_datos_fiscales WHERE tercero_id = %s", (c_id,))
        fiscal = cursor.fetchall()
        print(f"FISCAL_CLIENT_{c_id}:" + json.dumps(fiscal, default=decimal_default))
