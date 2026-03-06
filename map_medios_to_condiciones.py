from database import get_db_cursor

def map_medios_to_condiciones():
    with get_db_cursor(dictionary=True) as cursor:
        # Get all medios de pago
        cursor.execute("SELECT * FROM fin_medios_pago")
        medios = cursor.fetchall()
        
        for m in medios:
            # Check if this medio is already as a condition
            cursor.execute("SELECT id FROM fin_condiciones_pago WHERE enterprise_id = %s AND nombre = %s", 
                           (m['enterprise_id'], m['nombre']))
            if cursor.fetchone():
                print(f"Skipping {m['nombre']} for ent {m['enterprise_id']}, already exists.")
                continue

            # Invert recargo if it's meant to be a discount (user said "porcentaje de descuento")
            # But let's keep it as is. If they want discount, it's usually negative recargo or a separate field.
            # The schema has both fields.
            
            # Default logic: if it's CASH, maybe 10% discount.
            desc_pct = 0.0
            if 'EFECTIVO' in m['nombre'].upper() or m['tipo'] == 'EFECTIVO':
                desc_pct = 10.0
            
            cursor.execute("""
                INSERT INTO fin_condiciones_pago (enterprise_id, nombre, dias_vencimiento, descuento_pct, recargo_pct)
                VALUES (%s, %s, %s, %s, %s)
            """, (m['enterprise_id'], m['nombre'], 0, desc_pct, m['recargo_pct']))
            
        print("Completed mapping of Medios de Pago to Condiciones de Pago.")

if __name__ == "__main__":
    map_medios_to_condiciones()
