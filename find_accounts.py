import database
with database.get_db_cursor() as cur:
    cur.execute("SHOW TABLES")
    tables = [r[0] for r in cur.fetchall()]
    print("TABLES:", tables)
    
    # Common names for chart of accounts: con_cuentas, fin_cuentas, cont_cuentas
    for t in ['con_cuentas', 'fin_cuentas', 'cont_cuentas']:
        if t in tables:
            print(f"Searching in {t}...")
            cur.execute(f"SELECT id, nombre, codigo FROM {t} WHERE nombre LIKE '%mercaderia%' OR nombre LIKE '%transito%'")
            for r in cur.fetchall():
                print(r)
