from database import get_db_cursor
with get_db_cursor() as cursor:
    print("Normalizing collations to utf8mb4_unicode_ci...")
    
    # Update tables
    cursor.execute("ALTER TABLE erp_terceros_cm05 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    cursor.execute("ALTER TABLE sys_provincias CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    cursor.execute("ALTER TABLE log_erp_terceros_cm05 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    
    print("Collations updated successfully.")
