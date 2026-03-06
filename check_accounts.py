import database
with database.get_db_cursor(dictionary=True) as cur:
    cur.execute('SELECT id, nombre, codigo FROM con_cuentas WHERE nombre LIKE "%mercaderia%" OR nombre LIKE "%transito%" LIMIT 20')
    rows = cur.fetchall()
    for r in rows:
        print(f"{r['id']}: {r['nombre']} ({r['codigo']})")
