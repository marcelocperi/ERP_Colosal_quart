import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def migrate():
    with get_db_cursor() as cursor:
        print("Adding columns to erp_comprobantes...")
        try:
            cursor.execute("ALTER TABLE erp_comprobantes ADD COLUMN tipo_operacion ENUM('VENTA', 'COMPRA') DEFAULT 'VENTA' AFTER modulo")
        except Exception as e:
            print(f"Column tipo_operacion might already exist: {e}")
            
        try:
            cursor.execute("ALTER TABLE erp_comprobantes ADD COLUMN emisor_cuit VARCHAR(20) AFTER tipo_operacion")
        except Exception as e:
            print(f"Column emisor_cuit might already exist: {e}")

        try:
            cursor.execute("ALTER TABLE erp_comprobantes ADD COLUMN receptor_cuit VARCHAR(20) AFTER emisor_cuit")
        except Exception as e:
            print(f"Column receptor_cuit might already exist: {e}")

    print("Migration complete.")

if __name__ == '__main__':
    migrate()
