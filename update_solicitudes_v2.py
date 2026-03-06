from database import get_db_cursor
with get_db_cursor() as cursor:
    try:
        cursor.execute("""
            ALTER TABLE stk_devoluciones_solicitudes 
            ADD COLUMN entrega_persona_nombre VARCHAR(200) AFTER logistica_id,
            ADD COLUMN entrega_persona_doc_tipo VARCHAR(20) AFTER entrega_persona_nombre,
            ADD COLUMN entrega_persona_doc_nro VARCHAR(20) AFTER entrega_persona_doc_tipo
        """)
        print("Fields for delivery person added to stk_devoluciones_solicitudes.")
    except Exception as e:
        print(f"Error: {e}")
