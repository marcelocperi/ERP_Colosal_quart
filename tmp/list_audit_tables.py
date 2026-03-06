
from database import get_db_cursor
with get_db_cursor() as cursor:
    cursor.execute('SHOW TABLES')
    tables = [row[0] for row in cursor.fetchall()]
    
    print("Relevant tables found:")
    for t in tables:
        if any(keyword in t for keyword in ['compra', 'pago', 'movimiento', 'factura', 'stock']):
            print(f"- {t}")
