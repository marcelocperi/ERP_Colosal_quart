
from database import get_db_cursor
with get_db_cursor() as cursor:
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE COLUMN_NAME IN ('user_id', 'usuario_id', 'created_by') 
        AND (TABLE_NAME LIKE 'cmp%' OR TABLE_NAME LIKE 'fin%' OR TABLE_NAME LIKE 'stk%' OR TABLE_NAME LIKE 'stk%')
    """)
    rows = cursor.fetchall()
    print("Column Map for Audit:")
    for row in rows:
        print(f"{row[0]}: {row[1]}")
