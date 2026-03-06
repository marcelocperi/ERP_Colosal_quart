from database import get_db_cursor

def check_depositos():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM stk_depositos")
        deps = cursor.fetchall()
        print(f"\nTotal depósitos: {len(deps)}")
        for d in deps:
            print(f"- ID {d['id']}: {d['nombre']} (Empresa: {d['enterprise_id']}, Principal: {d['es_principal']})")

check_depositos()
