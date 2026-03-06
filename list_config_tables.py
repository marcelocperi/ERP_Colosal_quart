from database import get_db_cursor
with get_db_cursor() as c:
    c.execute("SHOW TABLES")
    tables = [r[0] for r in c.fetchall()]
    print("Tables:")
    for t in sorted(tables):
        if t.startswith('sys_') or 'config' in t or 'api' in t:
            print(f" - {t}")
