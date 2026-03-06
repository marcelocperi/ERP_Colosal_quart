from database import get_db_cursor
with get_db_cursor(dictionary=True) as c:
    c.execute('DESCRIBE stk_logisticas')
    fields = [r['Field'] for r in c.fetchall()]
    print(fields)
