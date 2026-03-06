import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    # 1. Total pending deep scan
    cursor.execute("SELECT COUNT(*) as count FROM stk_articulos WHERE enterprise_id = 1 AND api_checked < 2 AND codigo IS NOT NULL")
    count_pending = cursor.fetchone()['count']
    
    # 2. Total completed but without description 
    cursor.execute("""
        SELECT COUNT(*) as count 
        FROM stk_articulos 
        WHERE enterprise_id = 1 
        AND api_checked = 2 
        AND (JSON_EXTRACT(metadata_json, '$.descripcion') IS NULL 
             OR JSON_EXTRACT(metadata_json, '$.descripcion') = '' 
             OR JSON_EXTRACT(metadata_json, '$.descripcion') = 'null')
        AND codigo IS NOT NULL
    """)
    count_no_desc = cursor.fetchone()['count']
    
    print(f"Pendientes reales de Deep Scan (api_checked < 2): {count_pending}")
    print(f"Completados (api_checked = 2) PERO sin descripción: {count_no_desc}")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
