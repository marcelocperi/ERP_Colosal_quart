from database import get_db_cursor

try:
    with get_db_cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
        print("Tablas encontradas:", [t for t in tables if t.startswith('cmp_') or t.startswith('stk_') or t.startswith('erp_') or t.startswith('sys_')])
except Exception as e:
    print(f"Error: {e}")
