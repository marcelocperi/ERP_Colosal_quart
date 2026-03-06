import sys
sys.path.append('multiMCP')
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT id, nombre, condicion_iva FROM sys_enterprises WHERE nombre LIKE '%MARCELO%'")
    for r in cursor.fetchall():
        print(r)
