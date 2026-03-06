from database import get_db_cursor
with get_db_cursor() as cursor:
    cursor.execute("SHOW CREATE TABLE log_erp_terceros_cm05")
    print(cursor.fetchone()[1])
