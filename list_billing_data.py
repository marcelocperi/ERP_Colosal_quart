from database import get_db_cursor

def list_data():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- stk_tipos_articulo content ---")
        cursor.execute("SELECT * FROM stk_tipos_articulo")
        for r in cursor.fetchall():
            print(r)
        
        print("\n--- fin_condiciones_pago content ---")
        cursor.execute("SELECT * FROM fin_condiciones_pago")
        for r in cursor.fetchall():
            print(r)

        print("\n--- erp_terceros_condiciones content ---")
        cursor.execute("SELECT * FROM erp_terceros_condiciones")
        for r in cursor.fetchall():
            print(r)

if __name__ == "__main__":
    list_data()
