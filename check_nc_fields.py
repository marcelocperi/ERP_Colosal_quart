from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("DESCRIBE erp_comprobantes")
    for col in cursor.fetchall():
        if 'logistica' in col['Field'].lower() or 'transp' in col['Field'].lower():
            print(f"{col['Field']} - {col['Type']}")
