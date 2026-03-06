import os
import sys

# Ensure we can import from the current directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import get_db_cursor

def mass_update():
    with get_db_cursor() as cursor:
        # 1. Update simple condition
        cursor.execute("""
            UPDATE erp_terceros 
            SET condicion_pago_id = 1, 
                condicion_mixta_id = NULL
            WHERE enterprise_id = 0 AND es_cliente = 1
        """)
        updated = cursor.rowcount
        print(f"Se actualizaron {updated} clientes para empresa 0.")

if __name__ == "__main__":
    mass_update()
