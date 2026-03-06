import mariadb
from database import DB_CONFIG
import json

conn = mariadb.connect(**DB_CONFIG)
cur = conn.cursor(dictionary=True)

print("--- Service Efficiency ---")
cur.execute("SELECT service_name, hits_count, fields_provided, ebooks_provided FROM service_efficiency")
for row in cur.fetchall():
    print(row)

print("\n--- Articles with Ebook URL or local file ---")
cur.execute("""
    SELECT id, nombre, 
           JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.ebook_url')) as url,
           JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.archivo_local')) as local
    FROM stk_articulos 
    WHERE JSON_EXTRACT(metadata_json, '$.ebook_url') IS NOT NULL 
       OR JSON_EXTRACT(metadata_json, '$.archivo_local') IS NOT NULL
    LIMIT 20
""")
for row in cur.fetchall():
    print(row)

conn.close()
