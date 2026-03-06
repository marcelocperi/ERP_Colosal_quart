from database import get_db_cursor
import sys
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute('SELECT id, condicion_iva FROM sys_enterprises WHERE id=0')
    print("SYS ENTERPRISE ID=0:", cursor.fetchall())
    
    cursor.execute('SELECT id, condicion_iva FROM sys_enterprises')
    for row in cursor.fetchall():
        print("SYS ENTERPRISES:", row)
