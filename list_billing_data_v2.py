from database import get_db_cursor

def list_data():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- stk_tipos_articulo (id, ent, name, nature) ---")
        cursor.execute("SELECT id, enterprise_id, nombre, naturaleza FROM stk_tipos_articulo")
        for r in cursor.fetchall():
            print(f"{r['id']} | {r['enterprise_id']} | {r['nombre']} | {r['naturaleza']}")
        
        print("\n--- fin_condiciones_pago (id, ent, name, active) ---")
        cursor.execute("SELECT id, enterprise_id, nombre, activo FROM fin_condiciones_pago")
        for r in cursor.fetchall():
            print(f"{r['id']} | {r['enterprise_id']} | {r['nombre']} | {r['activo']}")

        print("\n--- erp_terceros_condiciones (tercero, cond, enabled) ---")
        cursor.execute("SELECT tercero_id, condicion_pago_id, habilitado FROM erp_terceros_condiciones")
        for r in cursor.fetchall():
            print(f"{r['tercero_id']} | {r['condicion_pago_id']} | {r['habilitado']}")

if __name__ == "__main__":
    list_data()
