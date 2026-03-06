
import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    # 1. Get Cuspide service ID
    cursor.execute("SELECT id FROM sys_external_services WHERE system_code = 'CUSPIDE_SCRAPE'")
    cuspide = cursor.fetchone()
    
    if not cuspide:
        print("Error: Servicio Cúspide no encontrado")
        conn.close()
        exit(1)
    
    cuspide_id = cuspide['id']
    print(f"Servicio Cúspide encontrado: ID {cuspide_id}")
    
    # 2. Update tipo_articulo "Libros" to use Cuspide
    cursor.execute("UPDATE stk_tipos_articulo SET usa_api_libros = 1 WHERE id = 1")
    print("Tipo 'Libros' actualizado para usar API externa")
    
    # 3. Clear existing service link for tipo 1
    cursor.execute("DELETE FROM stk_tipos_articulo_servicios WHERE tipo_articulo_id = 1 AND enterprise_id = 1")
    print("Configuración anterior eliminada")
    
    # 4. Link Cuspide to tipo "Libros"
    cursor.execute("""
        INSERT INTO stk_tipos_articulo_servicios (enterprise_id, tipo_articulo_id, servicio_id, es_primario)
        VALUES (1, 1, %s, 1)
    """, (cuspide_id,))
    print(f"Servicio Cúspide vinculado al tipo 'Libros'")
    
    conn.commit()
    conn.close()
    
    print("\n✅ Configuración completada exitosamente")
    print("El tipo 'Libros' ahora usará el scraper de Cúspide para enriquecer datos.")
    
except Exception as e:
    print(f"Error: {e}")
