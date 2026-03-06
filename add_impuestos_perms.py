from database import get_db_cursor
with get_db_cursor() as cursor:
    cursor.execute("INSERT IGNORE INTO sys_permissions (code, description, category, enterprise_id) VALUES ('view_ventas', 'Ver módulo de ventas', 'VENTAS', 0)")
    cursor.execute("INSERT IGNORE INTO sys_permissions (code, description, category, enterprise_id) VALUES ('admin_impuestos', 'Administración Impuestos', 'IMPUESTOS', 0)")
    cursor.execute("INSERT IGNORE INTO sys_permissions (code, description, category, enterprise_id) VALUES ('gestionar_cm05', 'Gestionar Convenio Multilateral', 'IMPUESTOS', 0)")
print("Permissions added")
