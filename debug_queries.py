import mariadb
from database import DB_CONFIG

conn = mariadb.connect(**DB_CONFIG)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM stk_articulos WHERE JSON_EXTRACT(metadata_json, '$.ebook_url') IS NOT NULL")
count = cur.fetchone()[0]
print(f"Total with ebook_url: {count}")

cur.execute("SELECT COUNT(*) FROM stk_articulos WHERE JSON_EXTRACT(metadata_json, '$.ebook_url') IS NOT NULL AND enterprise_id = 1")
count1 = cur.fetchone()[0]
print(f"Total with ebook_url in Ent 1: {count1}")

cur.execute("SELECT api_checked FROM stk_articulos WHERE id = 2")
status = cur.fetchone()[0]
print(f"Status of ID 2: {status}")

conn.close()
