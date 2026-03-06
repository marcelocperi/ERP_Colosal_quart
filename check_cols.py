from database import get_db_cursor
with get_db_cursor(dictionary=True) as c:
    c.execute('DESCRIBE erp_comprobantes')
    for r in c.fetchall():
        print(r['Field'])
