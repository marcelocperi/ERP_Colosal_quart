from database import get_db_cursor
with get_db_cursor() as cursor:
    try:
        cursor.execute("ALTER TABLE stk_devoluciones_solicitudes ADD COLUMN logistica_id INT AFTER condicion_devolucion_id")
        print("Column logistica_id added.")
    except Exception as e:
        print(f"Error or already exists: {e}")
