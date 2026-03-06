from database import get_db_cursor

def check_clients():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- erp_terceros (id, ent, name, cond_id, mixta_id) ---")
        cursor.execute("SELECT id, enterprise_id, nombre, condicion_pago_id, condicion_mixta_id FROM erp_terceros LIMIT 10")
        for r in cursor.fetchall():
            print(f"{r['id']} | {r['enterprise_id']} | {r['nombre']} | {r['condicion_pago_id']} | {r['condicion_mixta_id']}")

if __name__ == "__main__":
    check_clients()
