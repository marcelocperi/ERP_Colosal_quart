import sys
sys.path.append('multiMCP')
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT * FROM sys_tipos_comprobante WHERE codigo IN ('001', '011')")
    for r in cursor.fetchall():
        print(r)
