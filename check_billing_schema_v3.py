from database import get_db_cursor

def check():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- fin_medios_pago ---")
        cursor.execute("SELECT id, enterprise_id, nombre, tipo FROM fin_medios_pago")
        for r in cursor.fetchall():
            print(r)
            
        print("\n--- stk_depositos ---")
        cursor.execute("SELECT id, enterprise_id, nombre FROM stk_depositos")
        for r in cursor.fetchall():
            print(r)

if __name__ == "__main__":
    check()
