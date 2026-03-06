
from database import get_db_cursor

def find_calle_columns():
    with get_db_cursor() as cursor:
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE COLUMN_NAME LIKE '%calle%' AND TABLE_SCHEMA = DATABASE()
        """)
        results = cursor.fetchall()
        for table, col in results:
            print(f"Table: {table}, Column: {col}")

if __name__ == "__main__":
    find_calle_columns()
