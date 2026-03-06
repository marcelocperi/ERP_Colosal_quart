from database import get_db_cursor
with get_db_cursor() as c:
    c.execute("SHOW TABLES LIKE 'stk_%'")
    tables = [r[0] for r in c.fetchall()]
    print("Tables found:", ", ".join(tables))
    for table in tables:
        if table in ['stk_articulos', 'stk_depositos', 'stk_stock', 'stk_movimientos']:
            print(f"\n--- {table} ---")
            c.execute(f"DESCRIBE {table}")
            for col in c.fetchall():
                print(f"  {col[0]}: {col[1]}")
