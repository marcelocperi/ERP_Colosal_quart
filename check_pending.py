import mariadb
import sys
import os
from database import DB_CONFIG

enterprise_id = 1 # Change if needed

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    filter_clause = """
                    (JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.cover_url')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.cover_url')) = '') OR
                    (JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.descripcion')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.descripcion')) = '') OR
                    (JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.paginas')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.paginas')) = 0) OR
                    (JSON_EXTRACT(metadata_json, '$.temas') IS NULL OR JSON_LENGTH(JSON_EXTRACT(metadata_json, '$.temas')) = 0)
                """
    
    query = f"SELECT COUNT(*) as total FROM stk_articulos WHERE enterprise_id = %s AND codigo IS NOT NULL AND ({filter_clause})"
    cursor.execute(query, (enterprise_id,))
    row = cursor.fetchone()
    print(f"BOOKS MATCHING SMART FILTER (ENT {enterprise_id}): {row['total']}")
    
    # Check Normal filter
    query = "SELECT COUNT(*) as total FROM stk_articulos WHERE enterprise_id = %s AND (lengua IS NULL OR api_checked = 0) AND codigo IS NOT NULL"
    cursor.execute(query, (enterprise_id,))
    row = cursor.fetchone()
    print(f"BOOKS MATCHING NORMAL FILTER (ENT {enterprise_id}): {row['total']}")
    
    conn.close()
except Exception as e:
    print(f"ERROR: {e}")
