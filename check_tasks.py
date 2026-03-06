import mariadb
import sys
import os

# Add root dir to sys.path
sys.path.append(os.getcwd())

from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM sys_active_tasks")
    rows = cursor.fetchall()
    print("ACTIVE TASKS:")
    for r in rows:
        print(r)
    
    cursor.execute("SELECT * FROM system_stats WHERE key_name LIKE 'batch_%'")
    rows = cursor.fetchall()
    print("\nSYSTEM STATS:")
    for r in rows:
        print(r)
    
    conn.close()
except Exception as e:
    print(f"ERROR: {e}")
