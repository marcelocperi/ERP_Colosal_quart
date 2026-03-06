from database import get_db_cursor
with get_db_cursor(dictionary=True) as c:
    c.execute('DESCRIBE stk_logisticas')
    for r in c.fetchall():
        if r['Field'] == 'cuit':
            print(f"CUIT Type: {r['Type']}")
