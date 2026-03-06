from database import get_db_cursor
with get_db_cursor() as c:
    c.execute('SHOW TABLES')
    for r in c.fetchall():
        print(r[0])
