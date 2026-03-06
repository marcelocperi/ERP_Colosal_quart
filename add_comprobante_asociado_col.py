
from database import get_db_cursor
import sys

def run():
    try:
        with get_db_cursor() as cursor:
            # Check if column exists
            cursor.execute("SHOW COLUMNS FROM erp_comprobantes LIKE 'comprobante_asociado_id'")
            if not cursor.fetchone():
                print("Adding column comprobante_asociado_id...")
                cursor.execute("ALTER TABLE erp_comprobantes ADD COLUMN comprobante_asociado_id INT DEFAULT NULL AFTER asiento_id")
                print("Column added successfully.")
            else:
                print("Column already exists.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()
