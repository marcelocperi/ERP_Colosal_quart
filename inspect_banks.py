import database
with database.get_db_cursor() as cur:
    cur.execute("DESCRIBE fin_bancos")
    for r in cur.fetchall():
        print(r)
