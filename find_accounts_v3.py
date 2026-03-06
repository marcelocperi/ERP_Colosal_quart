import database
with database.get_db_cursor() as cur:
    cur.execute("SELECT id, nombre, codigo FROM cont_plan_cuentas WHERE codigo LIKE '1.4%%' OR nombre LIKE '%mercaderia%'")
    for r in cur.fetchall():
        print(r)
