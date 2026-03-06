import mariadb
import json
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    # Check articles that think they have a file
    cursor.execute("SELECT id, nombre, metadata_json, api_checked FROM stk_articulos WHERE JSON_EXTRACT(metadata_json, '$.archivo_local') = true")
    articles = cursor.fetchall()
    print(f"Total articles with archivo_local=true: {len(articles)}")
    
    for a in articles:
        # Check if file actually exists
        cursor.execute("SELECT id FROM stk_archivos_digitales WHERE articulo_id = %s", (a['id'],))
        if not cursor.fetchone():
            print(f"Article {a['id']} ({a['nombre']}) marked as having file, but IT IS MISSING.")
            
    # Check articles with ebook_url but no file
    cursor.execute("SELECT id, nombre, api_checked FROM stk_articulos WHERE JSON_EXTRACT(metadata_json, '$.ebook_url') IS NOT NULL")
    with_url = cursor.fetchall()
    print(f"Total articles with ebook_url: {len(with_url)}")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
