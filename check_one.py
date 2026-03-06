from database import get_db_cursor

def check_one_book():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre, isbn, numero_ejemplares FROM legacy_libros WHERE nombre LIKE '%Cien años%'")
        rows = cursor.fetchall()
        print("\n--- LEGACY LIBROS ---")
        for r in rows:
            print(f"- {r['nombre']} (ISBN: {r['isbn']}): {r['numero_ejemplares']} ejemplares")
        
        cursor.execute("SELECT id, nombre, codigo FROM stk_articulos WHERE nombre LIKE '%Cien años%'")
        rows = cursor.fetchall()
        print("\n--- STK ARTICULOS ---")
        for r in rows:
            print(f"- ID {r['id']}: {r['nombre']} (ISBN: {r['codigo']})")
            cursor.execute("SELECT * FROM stk_existencias WHERE articulo_id = %s", (r['id'],))
            ex = cursor.fetchall()
            for e in ex:
                print(f"  -> Deposito {e['deposito_id']}: {e['cantidad']} unidades")

check_one_book()
