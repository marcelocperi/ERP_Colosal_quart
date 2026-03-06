import sys
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute('SELECT emisor_condicion, receptor_condicion, allowed_codigos FROM sys_fiscal_comprobante_rules')
    data = cursor.fetchall()
    for row in data:
        sys.stdout.buffer.write(f"{row['emisor_condicion']} | {row['receptor_condicion']} | {row['allowed_codigos']}\n".encode('utf-8'))
