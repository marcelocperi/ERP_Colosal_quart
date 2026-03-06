
import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    # 1. Total de Articulos con ISBN (Potenciales candidatos)
    cursor.execute("SELECT COUNT(*) as total FROM stk_articulos WHERE enterprise_id = 1 AND codigo IS NOT NULL")
    total = cursor.fetchone()['total']
    
    # 2. Pendientes de Deep Scan (api_checked < 2)
    cursor.execute("SELECT COUNT(*) as pending FROM stk_articulos WHERE enterprise_id = 1 AND codigo IS NOT NULL AND api_checked < 2")
    pending = cursor.fetchone()['pending']
    
    # 3. Completados (api_checked = 2)
    cursor.execute("SELECT COUNT(*) as completed FROM stk_articulos WHERE enterprise_id = 1 AND api_checked = 2")
    completed = cursor.fetchone()['completed']

    # 4. Procesados en batch actual (system_stats)
    cursor.execute("SELECT value_int FROM system_stats WHERE key_name = 'batch_processed'")
    res = cursor.fetchone()
    processed_session = res['value_int'] if res else 0

    print(f"Total Libros con ISBN: {total}")
    print(f"Pendientes Deep Scan: {pending}")
    print(f"Completados Deep Scan: {completed}")
    print(f"Procesados Sesión Actual: {processed_session}")
    
    # Muestra de un par de registros completados para ver si tienen datos
    if completed > 0:
        cursor.execute("SELECT nombre, metadata_json FROM stk_articulos WHERE enterprise_id = 1 AND api_checked = 2 LIMIT 1")
        sample = cursor.fetchone()
        print("\nEjemplo procesado:")
        print(f"Nombre: {sample['nombre']}")
        print(f"Metadata: {sample['metadata_json']}")

    conn.close()
except Exception as e:
    print(f"Error: {e}")
