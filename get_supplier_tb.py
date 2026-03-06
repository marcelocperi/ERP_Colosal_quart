from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT error_traceback FROM sys_transaction_logs WHERE endpoint LIKE '%/compras/proveedores/perfil/469%' ORDER BY created_at DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        print(row['error_traceback'])
