from database import get_db_cursor
with get_db_cursor() as cursor:
    cursor.execute("SHOW CREATE TABLE erp_terceros_cm05")
    print("--- erp_terceros_cm05 ---")
    print(cursor.fetchone()[1])
    cursor.execute("SHOW CREATE TABLE sys_provincias")
    print("\n--- sys_provincias ---")
    print(cursor.fetchone()[1])
    
    cursor.execute("SELECT COLUMN_NAME, COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'erp_terceros_cm05' AND COLUMN_NAME = 'jurisdiccion_code'")
    print("\nCollation erp_terceros_cm05.jurisdiccion_code:", cursor.fetchone())
    
    cursor.execute("SELECT COLUMN_NAME, COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'sys_provincias' AND COLUMN_NAME = 'id'")
    print("Collation sys_provincias.id:", cursor.fetchone())
