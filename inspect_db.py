import database
import json

def inspect():
    with database.get_db_cursor() as cur:
        cur.execute("SHOW TABLES")
        tables = [r[0] for r in cur.fetchall()]
        
        print(f"Total tables: {len(tables)}")
        
        # Look for accounting related tables
        acct_tables = [t for t in tables if any(x in t for x in ['con_', 'fin_', 'cta', 'asiento', 'cuenta'])]
        print(f"Potential accounting tables: {acct_tables}")
        
        for t in acct_tables:
            try:
                # Try to see if it's the chart of accounts
                cur.execute(f"DESCRIBE {t}")
                cols = [c[0] for c in cur.fetchall()]
                if 'nombre' in cols and ('codigo' in cols or 'nro_cuenta' in cols):
                    print(f"\n--- Checking {t} ---")
                    cur.execute(f"SELECT id, nombre, codigo FROM {t} WHERE nombre LIKE '%mercaderia%' OR nombre LIKE '%transito%' OR nombre LIKE '%compra%'")
                    rows = cur.fetchall()
                    for r in rows:
                        print(r)
            except:
                continue

if __name__ == "__main__":
    inspect()
