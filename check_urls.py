import mariadb
import json
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, nombre, metadata_json FROM stk_articulos WHERE JSON_EXTRACT(metadata_json, '$.ebook_url') IS NOT NULL")
    rows = cursor.fetchall()
    for r in rows:
        meta = json.loads(r['metadata_json'])
        print(f"ID {r['id']}: {r['nombre']} -> {meta.get('ebook_url')}")
    conn.close()
except Exception as e:
    print(e)
