from database import get_db_cursor

def check_client_schema():
    with get_db_cursor() as cursor:
        cursor.execute("DESCRIBE erp_terceros")
        columns = cursor.fetchall()
        print("Schema for erp_terceros:")
        for col in columns:
            print(col)

if __name__ == "__main__":
    check_client_schema()
