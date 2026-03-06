import mariadb
import json
from database import DB_CONFIG

conn = mariadb.connect(**DB_CONFIG)
cur = conn.cursor(dictionary=True)

print("Repairing metadata booleans...")

cur.execute("SELECT id, metadata_json FROM stk_articulos WHERE metadata_json LIKE '%\"true\"%' OR metadata_json LIKE '%\"false\"%'")
rows = cur.fetchall()

repaired = 0
for row in rows:
    meta = json.loads(row['metadata_json'])
    changed = False
    for key in ['archivo_local', 'con_portada', 'con_descripcion']:
        if meta.get(key) == 'true':
            meta[key] = True
            changed = True
        elif meta.get(key) == 'false':
            meta[key] = False
            changed = True
            
    if changed:
        cur.execute("UPDATE stk_articulos SET metadata_json = %s WHERE id = %s", (json.dumps(meta), row['id']))
        repaired += 1

conn.commit()
print(f"Repaired {repaired} records.")
conn.close()
