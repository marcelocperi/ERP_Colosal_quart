from database import get_db_cursor
with get_db_cursor(dictionary=True) as c:
    c.execute("SELECT * FROM sys_invoice_layouts WHERE field_name IN ('label_cliente_domicilio', 'cliente_domicilio') AND enterprise_id = 0")
    for r in c.fetchall():
        print(r)
