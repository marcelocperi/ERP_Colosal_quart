from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("DESC erp_comprobantes")
    for r in cursor.fetchall():
        print(f"{r['Field']}: {r['Type']}")
