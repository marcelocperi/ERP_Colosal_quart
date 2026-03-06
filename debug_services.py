from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("DESCRIBE sys_external_services")
    with open("schema_services.txt", "w") as f:
        for row in cursor.fetchall():
            f.write(f"{row['Field']}: {row['Type']} (Null: {row['Null']}, Default: {row['Default']})\n")
