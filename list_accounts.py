from database import get_db_cursor
with get_db_cursor(dictionary=True) as c:
    c.execute("SELECT id, codigo, nombre FROM cont_plan_cuentas WHERE enterprise_id=0 OR enterprise_id=1")
    for r in c.fetchall():
        print(f"{r['id']}: {r['codigo']} - {r['nombre']}")
