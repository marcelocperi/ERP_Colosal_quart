from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("DESCRIBE stk_logisticas")
    for col in cursor.fetchall():
        print(f"{col['Field']} - {col['Type']}")
