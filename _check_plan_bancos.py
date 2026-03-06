from database import get_db_cursor

print("=== Cuentas 1.1.02 y 1.1.03 ===")
with get_db_cursor(dictionary=True) as c:
    c.execute("""
        SELECT id, enterprise_id, codigo, nombre, tipo, nivel, padre_id, imputable
        FROM cont_plan_cuentas
        WHERE codigo IN ('1.1.02','1.1.03','1.1')
        ORDER BY codigo
    """)
    for r in c.fetchall():
        print(r)

print("\n=== Columnas actuales de fin_bancos ===")
with get_db_cursor(dictionary=True) as c:
    c.execute("DESCRIBE fin_bancos")
    for r in c.fetchall():
        print(r['Field'], '-', r['Type'], '- NULL:', r['Null'], '- Default:', r['Default'])
