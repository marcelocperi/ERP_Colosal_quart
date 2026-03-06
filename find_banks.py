import database
with database.get_db_cursor() as cur:
    print("--- Searching for bank tables ---")
    cur.execute("SHOW TABLES")
    tables = [r[0] for r in cur.fetchall()]
    for t in tables:
        if any(x in t for x in ['banc', 'fin_']):
            print(f"Table: {t}")
