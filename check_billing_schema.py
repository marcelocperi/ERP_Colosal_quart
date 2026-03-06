from database import get_db_cursor

def check():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- stk_tipos_articulo ---")
        cursor.execute("SHOW COLUMNS FROM stk_tipos_articulo")
        for r in cursor.fetchall():
            print(f"{r['Field']}: {r['Type']}")
        
        print("\n--- stk_articulos ---")
        cursor.execute("SHOW COLUMNS FROM stk_articulos")
        for r in cursor.fetchall():
            print(f"{r['Field']}: {r['Type']}")

        print("\n--- erp_terceros ---")
        cursor.execute("SHOW COLUMNS FROM erp_terceros")
        for r in cursor.fetchall():
            print(f"{r['Field']}: {r['Type']}")

        print("\n--- fin_condiciones_pago ---")
        cursor.execute("SHOW COLUMNS FROM fin_condiciones_pago")
        for r in cursor.fetchall():
            print(f"{r['Field']}: {r['Type']}")

if __name__ == "__main__":
    check()
