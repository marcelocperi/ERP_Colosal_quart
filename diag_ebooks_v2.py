import mariadb
import json
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, api_checked, JSON_EXTRACT(metadata_json, '$.ebook_url') as url FROM stk_articulos WHERE JSON_EXTRACT(metadata_json, '$.ebook_url') IS NOT NULL")
    rows = cursor.fetchall()
    for r in rows:
        print(f"ID {r['id']}: api_checked={r['api_checked']}, url={r['url']}")
    conn.close()
except Exception as e:
    print(e)
