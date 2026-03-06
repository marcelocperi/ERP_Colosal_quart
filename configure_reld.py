
import mariadb
from database import DB_CONFIG

def configure():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # 1. Registrar el servicio Reld si no existe
        cursor.execute("SELECT id FROM sys_external_services WHERE system_code = 'RELD_SCRAPE'")
        reld_service = cursor.fetchone()
        
        if not reld_service:
            print("Registrando servicio Reld Scraper...")
            cursor.execute("""
                INSERT INTO sys_external_services 
                (enterprise_id, nombre, tipo_servicio, clase_implementacion, config_json, activo, modo_captura, url_objetivo, system_code)
                VALUES (1, 'Reld (Repuestos)', 'DATA_PROVIDER', 'services.scraping_service.ReldScraper', '{}', 1, 'SCRAPING', 'https://www.reld.com.ar', 'RELD_SCRAPE')
            """)
            reld_id = cursor.lastrowid
            print(f"Servicio Reld registrado con ID {reld_id}")
        else:
            reld_id = reld_service['id']
            print(f"Servicio Reld ya existe con ID {reld_id}. Actualizando URL y clase...")
            cursor.execute("""
                UPDATE sys_external_services 
                SET clase_implementacion = 'services.scraping_service.ReldScraper',
                    url_objetivo = 'https://www.reld.com.ar',
                    nombre = 'Reld (Repuestos)'
                WHERE id = %s
            """, (reld_id,))
            
        # 2. Vincular al tipo de artículo "Repuestos" (ID 2)
        print("Vinculando Reld al tipo de artículo 'Repuestos'...")
        
        # Primero habilitar APIs para el tipo 2
        cursor.execute("UPDATE stk_tipos_articulo SET usa_api_libros = 1 WHERE id = 2")
        
        # Limpiar vínculos previos para evitar duplicados
        cursor.execute("DELETE FROM stk_tipos_articulo_servicios WHERE tipo_articulo_id = 2 AND enterprise_id = 1")
        
        # Insertar nuevo vínculo como primario
        cursor.execute("""
            INSERT INTO stk_tipos_articulo_servicios (enterprise_id, tipo_articulo_id, servicio_id, es_primario)
            VALUES (1, 2, %s, 1)
        """, (reld_id,))
        
        conn.commit()
        conn.close()
        print("\n✅ Configuración de Reld Scraper completada exitosamente.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    configure()
