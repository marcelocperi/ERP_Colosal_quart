import database
with database.get_db_cursor() as cur:
    print("--- Searching in cont_plan_cuentas ---")
    cur.execute("SELECT id, nombre, codigo FROM cont_plan_cuentas WHERE nombre LIKE '%%' LIMIT 500")
    rows = cur.fetchall()
    for r in rows:
        if any(x in r[1].lower() for x in ['mercaderia', 'transito', 'importacion', 'stock', 'proveedor']):
            print(r)
