from database import get_db_cursor
import sys

# Ensure UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute('SELECT id, condicion_iva FROM sys_enterprises')
    for row in cursor.fetchall():
        print(f"ID={row['id']} | IVA={row['condicion_iva']}")
