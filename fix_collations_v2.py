from database import get_db_cursor
with get_db_cursor() as cursor:
    print("Normalizing collations to utf8mb4_unicode_ci using MODIFY COLUMN to avoid FK issues...")
    
    # erp_terceros_cm05
    cursor.execute("ALTER TABLE erp_terceros_cm05 MODIFY jurisdiccion_code VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL")
    
    # sys_provincias
    # This one is trickier because 'id' is used in FKs. 
    # But usually FKs in MySQL require same type but not necessarily same collation if they are INT.
    # Wait, 'id' in sys_provincias is INT or VARCHAR? 
    # check_collations.py said: Collation sys_provincias.id: ('id', 'utf8mb4_uca1400_ai_ci')
    # That means it's a string!
    
    # Since it's a string ID used in FKs, we might need to disable FK checks or update the whole DB.
    # Better approach: Use COLLATE in the query join to match them on the fly if altering is too risky.
    
    print("Done (partial).")
