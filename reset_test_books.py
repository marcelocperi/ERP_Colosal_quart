import mariadb
from database import DB_CONFIG

conn = mariadb.connect(**DB_CONFIG)
cur = conn.cursor()

# Reset books that have an ebook URL but no local file, to retry with improved parsing
cur.execute("""
    UPDATE stk_articulos 
    SET api_checked = 0 
    WHERE enterprise_id = 1 
    AND JSON_EXTRACT(metadata_json, '$.ebook_url') IS NOT NULL 
    AND (
        JSON_EXTRACT(metadata_json, '$.archivo_local') IS NULL 
        OR JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.archivo_local')) = 'false'
        OR JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.archivo_local')) = '0'
    )
""")
conn.commit()
print(f"Reset {cur.rowcount} books for re-processing.")
conn.close()
