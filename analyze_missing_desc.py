import mariadb
import json
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT id, metadata_json FROM stk_articulos WHERE enterprise_id = 1 AND api_checked = 2")
    rows = cursor.fetchall()
    
    missing = 0
    for row in rows:
        meta = json.loads(row['metadata_json']) if row['metadata_json'] else {}
        desc = meta.get('descripcion')
        
        is_missing = False
        if not desc:
            is_missing = True
        elif isinstance(desc, str) and (desc.strip() == '' or desc.lower() == 'null'):
            is_missing = True
        elif isinstance(desc, dict) and not desc.get('value'):
            is_missing = True
            
        if is_missing:
            missing += 1
            
    print(f"Total analizados: {len(rows)}")
    print(f"Total considerados 'Sin descripción': {missing}")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
