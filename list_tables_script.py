from database import get_db_cursor

def list_tables():
    with get_db_cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        print("All tables:")
        for t in tables:
            if t.startswith('fin_') or t.startswith('ventas_') or t.startswith('cmp_'):
                print(t)

if __name__ == "__main__":
    list_tables()
