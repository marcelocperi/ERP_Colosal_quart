from database import get_db_cursor
def d(table):
    with get_db_cursor(dictionary=True) as c:
        print(f"\n--- {table} ---")
        c.execute(f"DESC {table}")
        for r in c.fetchall():
            print(f"  {r['Field']} ({r['Type']}) - Null: {r['Null']}, Key: {r['Key']}")

d('sys_config_fiscal')
d('sys_external_services')
d('stk_servicios_config')
