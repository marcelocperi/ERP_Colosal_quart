import mariadb
import json
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT id, codigo, metadata_json FROM stk_articulos WHERE enterprise_id = 1 AND api_checked = 2 LIMIT 10")
    rows = cursor.fetchall()
    
    for row in rows:
        meta = json.loads(row['metadata_json']) if row['metadata_json'] else {}
        desc = meta.get('descripcion')
        print(f"ID: {row['id']} | ISBN: {row['codigo']} | Desc: {type(desc)} | Val: {str(desc)[:30]}")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
