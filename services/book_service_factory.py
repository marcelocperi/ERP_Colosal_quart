
from services.library_api_service import get_book_info_by_isbn
import mariadb
from database import DB_CONFIG

class BookServiceFactory:
    """
    Factory to retrieve the appropriate data service for a given Article Type.
    """
    
    @staticmethod
    def get_service_for_type(tipo_articulo_id, enterprise_id):
        # 1. Look up configuration in DB
        try:
            conn = mariadb.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT s.nombre, s.tipo_servicio, s.clase_implementacion, s.config_json, s.activo
                FROM stk_tipos_articulo_servicios tas
                JOIN sys_external_services s ON tas.servicio_id = s.id
                WHERE tas.tipo_articulo_id = %s AND tas.enterprise_id = %s AND tas.es_primario = 1
                  AND s.activo = 1
            """, (tipo_articulo_id, enterprise_id))
            
            config = cursor.fetchone()
            conn.close()
            
            if not config:
                return NativeService()
                
            if config['nombre'] == 'OpenLibraryService':
                return OpenLibraryService()
            
            # Support for Cuspide Scraper
            if config['clase_implementacion'] == 'services.scraping_service.CuspideScraper':
                from services.scraping_service import CuspideScraper
                return CuspideScraper()
                
            # Generic catch-all for implemented classes
            if config['clase_implementacion']:
                # Dynamically try to import
                try:
                    module_name, class_name = config['clase_implementacion'].rsplit('.', 1)
                    module = __import__(module_name, fromlist=[class_name])
                    return getattr(module, class_name)()
                except Exception as e:
                    print(f"Error loading service {config['clase_implementacion']}: {e}")
                    return NativeService()
                
            # Future: Dynamic loading based on clase_implementacion
            return NativeService()
            
        except Exception as e:
            print(f"Factory Error: {e}")
            return NativeService()

class NativeService:
    def get_info(self, identifier):
        return False, "Servicio Nativo: Ingreso manual requerido."

class OpenLibraryService:
    def get_info(self, isbn):
        return get_book_info_by_isbn(isbn)
