from database import get_db_cursor
with get_db_cursor() as cursor:
    cursor.execute("SHOW CREATE TABLE erp_puestos")
    print(cursor.fetchone()[1])
    cursor.execute("SHOW CREATE TABLE erp_contactos")
    print(cursor.fetchone()[1])
    cursor.execute("SHOW CREATE TABLE erp_direcciones")
    print(cursor.fetchone()[1])
