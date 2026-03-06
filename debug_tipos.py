import sys
sys.path.append('multiMCP')
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute('SELECT * FROM sys_tipos_comprobante')
    for r in cursor.fetchall():
        print(r)
