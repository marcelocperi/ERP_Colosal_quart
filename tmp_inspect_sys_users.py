import traceback
from database import get_db_cursor

try:
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SHOW CREATE TABLE sys_users")
        res = cursor.fetchone()
        with open("schema_sys_users.txt", "w", encoding="utf-8") as f:
            if res:
                f.write("=== SYS_USERS SCHEMA ===\n")
                f.write(str(res.get('Create Table', res)) + "\n")
            else:
                f.write("Table sys_users not found.\n")
except Exception as e:
    with open("schema_sys_users.txt", "w", encoding="utf-8") as f:
        f.write(traceback.format_exc())
