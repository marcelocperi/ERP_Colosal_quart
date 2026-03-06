import mariadb
from database import DB_CONFIG

conn = mariadb.connect(**DB_CONFIG)
cur = conn.cursor()

# Reset ALL books with ebook_url for enterprise 1
cur.execute("""
    UPDATE stk_articulos 
    SET api_checked = 0 
    WHERE enterprise_id = 1 
    AND JSON_EXTRACT(metadata_json, '$.ebook_url') IS NOT NULL
""")
conn.commit()
print(f"Reset {cur.rowcount} books for re-processing.")
conn.close()
