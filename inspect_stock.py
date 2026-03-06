from database import get_db_cursor
with get_db_cursor() as c:
    c.execute("SHOW TABLES LIKE 'stk_%'")
    for r in c.fetchall():
        print(f"Table: {r[0]}")
        c.execute(f"DESCRIBE {r[0]}")
        for col in c.fetchall():
            print(f"  - {col[0]} ({col[1]})")
