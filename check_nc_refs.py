from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("DESCRIBE erp_comprobantes")
    for col in cursor.fetchall():
        if any(kw in col['Field'].lower() for kw in ['ref', 'asoc', 'orig', 'fact']):
            print(f"{col['Field']} - {col['Type']}")
