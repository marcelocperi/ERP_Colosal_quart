from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SHOW TABLES LIKE 'fin_factura_cobros'")
    if cursor.fetchone():
        cursor.execute("DESCRIBE fin_factura_cobros")
        for col in cursor.fetchall():
            print(f"{col['Field']} - {col['Type']}")
    else:
        print("Table fin_factura_cobros does not exist.")
