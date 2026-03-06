import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("UPDATE stk_articulos SET api_checked = 0 WHERE JSON_EXTRACT(metadata_json, '$.ebook_url') IS NOT NULL")
    conn.commit()
    print(f"Reset {cursor.rowcount} articles with ebook_url to api_checked=0")
    conn.close()
except Exception as e:
    print(e)
