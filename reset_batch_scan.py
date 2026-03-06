
import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Reset limit 50 articles that don't have a description in metadata
    query = """
    UPDATE stk_articulos 
    SET api_checked = 0 
    WHERE enterprise_id = 1 
    AND codigo IS NOT NULL 
    AND (
        metadata_json IS NULL 
        OR JSON_EXTRACT(metadata_json, '$.descripcion') IS NULL 
        OR JSON_EXTRACT(metadata_json, '$.descripcion') = ''
    )
    LIMIT 50
    """
    
    cursor.execute(query)
    count = cursor.rowcount
    
    conn.commit()
    conn.close()
    
    print(f"Se han restablecido {count} artículos para ser escaneados nuevamente (Escaneo Profundo).")
    
except Exception as e:
    print(f"Error: {e}")
