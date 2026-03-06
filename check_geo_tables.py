
from database import get_db_cursor

def check_tables():
    with get_db_cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [r[0] for r in cursor.fetchall()]
        print("--- TODAS LAS TABLAS ---")
        for t in sorted(tables):
            print(t)
        print("------------------------")
        
        for t in ['sys_calles', 'sys_localidades', 'sys_municipios', 'sys_provincias']:
            if t in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {t}")
                count = cursor.fetchone()[0]
                print(f"Table {t} exists with {count} rows.")
            else:
                print(f"Table {t} does NOT exist.")

if __name__ == "__main__":
    check_tables()
