import mariadb
import sys
import os
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT id, enterprise_id, full_name, username FROM sys_usuarios")
    rows = cursor.fetchall()
    print("USERS:")
    for r in rows:
        print(r)
    
    conn.close()
except Exception as e:
    print(f"ERROR: {e}")
