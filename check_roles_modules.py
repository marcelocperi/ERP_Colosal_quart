import sys
import pprint
from database import get_db_cursor

try:
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SHOW TABLES LIKE 'sys_%'")
        tables = [list(r.values())[0] for r in cursor.fetchall()]
        print("SYS TABLES:", tables)
        for t in ['sys_roles', 'sys_modules', 'sys_role_modules', 'sys_permissions', 'sys_role_permissions']:
            if t in tables:
                cursor.execute(f"DESCRIBE {t}")
                print(f"--- {t} ---")
                for row in cursor.fetchall():
                    print(row['Field'], row['Type'])
                
                cursor.execute(f"SELECT COUNT(*) as c FROM {t}")
                print("Count:", cursor.fetchone()['c'])
except Exception as e:
    print(f"Error: {e}")
