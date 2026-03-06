from database import get_db_cursor

def seed_condiciones_pago():
    with get_db_cursor(dictionary=True) as cursor:
        # Get all distinct enterprise_ids that have medios de pago
        cursor.execute("SELECT DISTINCT enterprise_id FROM fin_medios_pago")
        enterprises = [row['enterprise_id'] for row in cursor.fetchall()]
        
        # If no enterprises found, at least do 0
        if 0 not in enterprises:
            enterprises.append(0)

        for ent_id in enterprises:
            # Check if already seeded
            cursor.execute("SELECT COUNT(*) as count FROM fin_condiciones_pago WHERE enterprise_id = %s", (ent_id,))
            if cursor.fetchone()['count'] > 0:
                print(f"Skipping enterprise {ent_id}, already has data.")
                continue

            # Default conditions to create
            condiciones = [
                {'nombre': 'Contado Efectivo', 'dias': 0, 'desc': 10.0, 'rec': 0.0},
                {'nombre': 'Contado Transf/Debito', 'dias': 0, 'desc': 5.0, 'rec': 0.0},
                {'nombre': 'Tarjeta 1 cuota', 'dias': 0, 'desc': 0.0, 'rec': 0.0},
                {'nombre': 'Tarjeta 3 cuotas', 'dias': 0, 'desc': 0.0, 'rec': 15.0},
                {'nombre': 'Tarjeta 6 cuotas', 'dias': 0, 'desc': 0.0, 'rec': 30.0},
                {'nombre': 'Cuenta Corriente 30 dias', 'dias': 30, 'desc': 0.0, 'rec': 0.0},
                {'nombre': 'Cuenta Corriente 60 dias', 'dias': 60, 'desc': 0.0, 'rec': 5.0},
                {'nombre': 'Cheque al Dia', 'dias': 0, 'desc': 0.0, 'rec': 0.0},
                {'nombre': 'Cheque 30 dias', 'dias': 30, 'desc': 0.0, 'rec': 0.0},
            ]

            for c in condiciones:
                cursor.execute("""
                    INSERT INTO fin_condiciones_pago (enterprise_id, nombre, dias_vencimiento, descuento_pct, recargo_pct)
                    VALUES (%s, %s, %s, %s, %s)
                """, (ent_id, c['nombre'], c['dias'], c['desc'], c['rec']))
            
            print(f"Enterprise {ent_id} seeded with default condiciones de pago.")

if __name__ == "__main__":
    seed_condiciones_pago()
