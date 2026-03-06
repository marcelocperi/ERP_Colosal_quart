from database import get_db_cursor
with get_db_cursor() as cursor:
    cursor.execute("SHOW FULL COLUMNS FROM sys_provincias WHERE Field = 'id'")
    print("sys_provincias.id:", cursor.fetchone())
    cursor.execute("SHOW FULL COLUMNS FROM erp_terceros_cm05 WHERE Field = 'jurisdiccion_code'")
    print("erp_terceros_cm05.jurisdiccion_code:", cursor.fetchone())
