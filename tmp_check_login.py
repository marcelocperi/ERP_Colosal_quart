import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

conn = pymysql.connect(
    host=os.environ.get("DB_HOST", "localhost"),
    port=int(os.environ.get("DB_PORT", "3307")),
    user=os.environ.get("DB_USER", "root"),
    password=os.environ.get("DB_PASSWORD", ""),
    database=os.environ.get("DB_NAME", "multi_mcp_db"),
    cursorclass=pymysql.cursors.DictCursor
)

try:
    with conn.cursor() as c:
        c.execute("SELECT id, nombre, estado FROM sys_enterprises ORDER BY id LIMIT 10")
        print("=== EMPRESAS ===")
        for r in c.fetchall():
            print(r)

        c.execute("SELECT id, username, enterprise_id, LENGTH(password_hash) as pwd_len FROM sys_users LIMIT 10")
        print("\n=== USUARIOS ===")
        for r in c.fetchall():
            print(r)
finally:
    conn.close()
