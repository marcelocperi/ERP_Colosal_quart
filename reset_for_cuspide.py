import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Resetear api_checked a 1 para libros que no tienen descripción
    # Esto permite que el Escaneo Profundo los vuelva a tomar
    query = """
        UPDATE stk_articulos 
        SET api_checked = 1 
        WHERE enterprise_id = 1 
        AND api_checked = 2 
        AND (JSON_EXTRACT(metadata_json, '$.descripcion') IS NULL 
             OR JSON_EXTRACT(metadata_json, '$.descripcion') = '' 
             OR JSON_EXTRACT(metadata_json, '$.descripcion') = 'null')
        AND codigo IS NOT NULL
    """
    
    cursor.execute(query)
    affected = cursor.rowcount
    
    conn.commit()
    conn.close()
    print(f"✓ Se resetearon {affected} libros para volver a escanear (sin descripción).")
    print("Ahora puedes iniciar el 'Escaneo Profundo' desde la web.")
except Exception as e:
    # Fix typo in conn.close if I made one (I wrote clase)
    try: conn.close()
    except: pass
    print(f"Error: {e}")
