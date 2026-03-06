import mariadb
import sys
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("Limpiando archivos HTML erróneos...")
    
    # 1. Identificar artículos afectados
    cursor.execute("SELECT articulo_id FROM stk_archivos_digitales WHERE formato = 'html'")
    rows = cursor.fetchall()
    ids = [r[0] for r in rows]
    
    if not ids:
        print("No hay archivos HTML basura.")
    else:
        print(f"Encontrados {len(ids)} artículos con archivos basura.")
        
        # 2. Resetear api_checked y metadata
        placeholders = ', '.join(['%s'] * len(ids))
        cursor.execute(f"UPDATE stk_articulos SET api_checked = 0, metadata_json = JSON_SET(metadata_json, '$.archivo_local', 'false', '$.ebook_url', NULL) WHERE id IN ({placeholders})", ids)
        
        # 3. Eliminar archivos
        cursor.execute(f"DELETE FROM stk_archivos_digitales WHERE formato = 'html'")
        
        conn.commit()
        print("Limpieza completada exitosamente.")
        
    conn.close()

except Exception as e:
    print(f"Error: {e}")
