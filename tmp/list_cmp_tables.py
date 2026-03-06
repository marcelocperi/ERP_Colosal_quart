
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SHOW TABLES LIKE 'cmp_%'")
    tables = [list(r.values())[0] for r in cursor.fetchall()]
    print(f"Tablas encontradas: {tables}")
