import sys
from database import get_db_cursor

try:
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SHOW TABLES LIKE 'sys_%'")
        tables = [list(r.values())[0] for r in cursor.fetchall()]
        print("SYS TABLES:")
        for t in tables:
            print(t)
        print("\nSCHEMAS:")
        for t in tables:
            cursor.execute(f"DESCRIBE {t}")
            print(f"\n--- {t} ---")
            for row in cursor.fetchall():
                print(row['Field'], row['Type'])
except Exception as e:
    print(f"Error: {e}")
