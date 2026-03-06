from database import get_db_cursor
try:
    with get_db_cursor() as cursor:
        cursor.execute("""
            ALTER TABLE sys_enterprises 
            ADD COLUMN afip_crt TEXT, 
            ADD COLUMN afip_key TEXT, 
            ADD COLUMN afip_entorno VARCHAR(20) DEFAULT 'testing'
        """)
    print("Columnas añadidas exitosamente")
except Exception as e:
    print(f"Error o ya existen: {e}")
