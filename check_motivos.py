from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT id, nombre FROM stk_motivos")
    for row in cursor.fetchall():
        print(f"{row['id']} - {row['nombre']}")
