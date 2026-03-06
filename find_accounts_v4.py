import database
with database.get_db_cursor() as cur:
    print("--- Searching for specific accounts ---")
    queries = [
        "SELECT id, nombre, codigo FROM cont_plan_cuentas WHERE nombre LIKE '%exterior%'",
        "SELECT id, nombre, codigo FROM cont_plan_cuentas WHERE nombre LIKE '%extranjero%'",
        "SELECT id, nombre, codigo FROM cont_plan_cuentas WHERE nombre LIKE '%gasto%' LIMIT 10",
        "SELECT id, nombre, codigo FROM cont_plan_cuentas WHERE nombre LIKE '%costo%' LIMIT 10"
    ]
    for q in queries:
        print(f"\nQuery: {q}")
        cur.execute(q)
        for r in cur.fetchall():
            print(r)
