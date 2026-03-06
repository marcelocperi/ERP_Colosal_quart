from database import get_db_cursor
import json
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("""
        SELECT t.id, t.nombre, t.cuit, df.jurisdiccion, df.alicuota 
        FROM erp_terceros t 
        JOIN erp_datos_fiscales df ON t.id = df.tercero_id 
        WHERE t.es_cliente = 1 AND (df.jurisdiccion LIKE '%ARBA%' OR df.jurisdiccion LIKE '%AGIP%' OR df.jurisdiccion LIKE '%CABA%')
    """)
    clients = cursor.fetchall()
    print("IIBB_CLIENTS:" + json.dumps(clients, default=decimal_default))
