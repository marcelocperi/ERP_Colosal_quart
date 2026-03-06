from database import get_db_cursor
from services.sod_service import initialize_sod_structure

with get_db_cursor() as cursor:
    cursor.execute("SELECT id FROM sys_enterprises")
    enterprises = cursor.fetchall()

for e in enterprises:
    try:
        initialize_sod_structure(e[0])
        print(f"Updated SOD for enterprise {e[0]}")
    except Exception as ex:
        print(f"Failed for enterprise {e[0]}: {ex}")

print("Done updating SoD across all enterprises")
