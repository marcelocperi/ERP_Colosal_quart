from database import get_db_cursor

def list_articles():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- stk_articulos (id, ent, name, type_id, nature) ---")
        cursor.execute("SELECT id, enterprise_id, nombre, tipo_articulo_id, naturaleza FROM stk_articulos LIMIT 20")
        for r in cursor.fetchall():
            print(f"{r['id']} | {r['enterprise_id']} | {r['nombre']} | {r['tipo_articulo_id']} | {r['naturaleza']}")

if __name__ == "__main__":
    list_articles()
