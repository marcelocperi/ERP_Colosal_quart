from database import get_db_cursor
import sys

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print([list(t.values())[0] for t in tables if 'log' in list(t.values())[0].lower() or 'incident' in list(t.values())[0].lower() or 'error' in list(t.values())[0].lower()])
