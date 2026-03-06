from database import get_db_cursor

with get_db_cursor(dictionary=True) as c:
    c.execute('SELECT id, code, description, category FROM sys_permissions WHERE enterprise_id=0 ORDER BY category, code')
    rows = c.fetchall()
    for r in rows:
        cat = (r['category'] or 'SIN CATEGORIA').ljust(22)
        code = (r['code'] or '').ljust(36)
        print(f"{cat} | {code} | {r['description']}")
