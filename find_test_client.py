from database import get_db_cursor
import json
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

with get_db_cursor(dictionary=True) as cursor:
    # Look for any client with perception rates
    cursor.execute("""
        SELECT t.id, t.nombre, t.cuit, df.jurisdiccion, df.alicuota 
        FROM erp_terceros t 
        JOIN erp_datos_fiscales df ON t.id = df.tercero_id 
        WHERE t.es_cliente = 1 AND df.alicuota > 0
    """)
    clients = cursor.fetchall()
    print("MATCHING_CLIENTS:" + json.dumps(clients, default=decimal_default))
    
    # Check current enterprise agent status
    cursor.execute("SELECT * FROM sys_enterprises_fiscal")
    agents = cursor.fetchall()
    print("AGENT_STATUS:" + json.dumps(agents, default=decimal_default))
