from database import get_db_cursor
with get_db_cursor() as c:
    c.execute('SHOW TABLES')
    tables = [r[0] for r in c.fetchall()]
    for t in tables:
        if 'logistica' in t.lower() or 'transport' in t.lower() or 'fletero' in t.lower():
            print(f"Match: {t}")
