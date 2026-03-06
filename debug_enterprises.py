from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute('SELECT id, nombre, cuit, condicion_iva FROM sys_enterprises')
    for r in cursor.fetchall():
        print(r)
