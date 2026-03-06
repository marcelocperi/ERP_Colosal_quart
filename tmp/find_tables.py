from database import get_db_cursor
import sys

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SHOW TABLES LIKE '%error%'")
    tables1 = cursor.fetchall()
    print("Error tables:", tables1)

    cursor.execute("SHOW TABLES LIKE '%incident%'")
    tables2 = cursor.fetchall()
    print("Incident tables:", tables2)

    cursor.execute("SHOW TABLES LIKE '%log%'")
    tables3 = cursor.fetchall()
    print("Log tables:", tables3)
