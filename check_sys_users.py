import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("DESCRIBE sys_users")
    for row in cursor.fetchall():
        print(row)
    conn.close()
except Exception as e:
    print(f"Error: {e}")
