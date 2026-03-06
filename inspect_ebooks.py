import mariadb
import sys
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT id, articulo_id, formato, nombre_archivo, LENGTH(contenido) as size FROM stk_archivos_digitales LIMIT 5")
    rows = cursor.fetchall()
    
    print(f"Total filas: {len(rows)}")
    for r in rows:
        print(r)
        
    conn.close()
except Exception as e:
    print(e)
