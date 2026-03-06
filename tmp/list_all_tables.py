
from database import get_db_cursor
with get_db_cursor() as cursor:
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()")
    tables = [r[0] for r in cursor.fetchall()]
    for t in sorted(tables):
        print(t)
