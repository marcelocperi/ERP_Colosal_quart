from database import get_db_cursor
with get_db_cursor() as cursor:
    try:
        cursor.execute("ALTER TABLE erp_comprobantes ADD COLUMN logistica_id INT AFTER condicion_pago_id")
        print("Column logistica_id added to erp_comprobantes.")
    except Exception as e:
        print(f"Error or already exists: {e}")
