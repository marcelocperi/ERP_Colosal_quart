import database
with database.get_db_cursor() as cur:
    for table in ['cont_asientos', 'cont_asientos_detalle']:
        print(f"\n--- Structure of {table} ---")
        cur.execute(f"DESCRIBE {table}")
        for col in cur.fetchall():
            print(col)
