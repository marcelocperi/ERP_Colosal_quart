from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute('SELECT id, nombre, condicion_iva FROM sys_enterprises')
    for row in cursor.fetchall():
        print(row)
        
    cursor.execute('SELECT id, username, enterprise_id FROM sys_users')
    print("\nUsers:")
    for row in cursor.fetchall():
        print(row)
