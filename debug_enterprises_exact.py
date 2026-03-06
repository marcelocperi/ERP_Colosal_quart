import sys
sys.path.append('multiMCP')
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute('SELECT id, nombre, condicion_iva FROM sys_enterprises')
    for r in cursor.fetchall():
        print(f"ID: {r['id']} | NAME: {r['nombre']} | IVA: [{r['condicion_iva']}]")
