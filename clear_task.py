import mariadb
from database import DB_CONFIG

conn = mariadb.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("DELETE FROM sys_active_tasks WHERE task_id='enrich_1'")
conn.commit()
print("Cleared task enrich_1")
conn.close()
