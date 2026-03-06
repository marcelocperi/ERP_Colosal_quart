from database import get_db_cursor
with get_db_cursor(dictionary=True) as c:
    c.execute("SELECT id, name FROM sys_roles WHERE enterprise_id=0")
    roles = c.fetchall()
    for r in roles:
        print(f"ID: {r['id']}, NAME: {r['name']}")
