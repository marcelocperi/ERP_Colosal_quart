
import json
import logging
import time
from datetime import datetime
from services import library_api_service
from core.security_utils import sanitize_filename
from services.book_service_factory import BookServiceFactory, NativeService

logger = logging.getLogger('enrichment_processor')

class BookEnrichmentProcessor:
    def __init__(self, conn, efficiency_mgr):
        self.conn = conn
        self.cursor = conn.cursor(dictionary=True)
        self.efficiency_mgr = efficiency_mgr
        
        # Mapeo estático de servicios disponibles
        self.service_map = {
            'Mercado Libre': ('Mercado Libre', 'services.scraping_service', 'MercadoLibreScraper'),
            'Google Books': ('Google Books', 'services.library_api_service', 'get_book_info_from_google_books'),
            'Librario': ('Librario', 'services.library_api_service', 'get_book_info_from_librario'),
            'Open Library': ('Open Library', 'services.library_api_service', 'get_book_info_by_isbn'),
            'WorldCat': ('WorldCat', 'services.library_api_service', 'get_book_info_from_worldcat'),
            'Cúspide': ('Cúspide', 'services.scraping_service', 'CuspideScraper'),
            'Amazon': ('Amazon', 'services.scraping_service', 'AmazonScraper'),
            'Reld': ('Reld', 'services.scraping_service', 'ReldScraper')
        }

    def get_existing_files_map(self, book_ids, enterprise_id=None):
        """Optimización N+1: Obtiene todos los IDs de libros que ya tienen archivos digitalizados."""
        if not book_ids:
            return set()
        
        format_strings = ','.join(['%s'] * len(book_ids))
        query = f"SELECT articulo_id FROM stk_archivos_digitales WHERE articulo_id IN ({format_strings})"
        params = list(book_ids)

        if enterprise_id is not None:
             query += " AND enterprise_id = %s"
             params.append(enterprise_id)

        self.cursor.execute(query, tuple(params))
        return {row['articulo_id'] for row in self.cursor.fetchall()}

    def build_execution_plan(self, lib, enterprise_id, db_ranking, deep_scan=False):
        """Construye el plan de ejecución dinámico para un libro específico."""
        user_sequence = [
            'Mercado Libre', 'Google Books', 'Librario', 'Open Library', 
            'Amazon', 'WorldCat', 'Cúspide'
        ]
        
        # 1. Prioridad oficial por tipo de artículo
        tipo_id = lib.get('tipo_articulo_id', 1)
        service_primario = BookServiceFactory.get_service_for_type(tipo_id, enterprise_id)
        
        class_to_label = {v[2]: k for k, v in self.service_map.items()}
        official_label = class_to_label.get(service_primario.__class__.__name__)

        # 2. Ensamblar secuencia respetando el orden del usuario pero inyectando el ranking de eficiencia
        execution_plan = []
        for name in user_sequence:
            if name in self.service_map:
                execution_plan.append(self.service_map[name])
        
        # 3. Agregar otros del ranking o pool si no están
        pool = (db_ranking or []) + user_sequence
        if official_label: pool.insert(0, official_label)
        
        for name in pool:
            if name in self.service_map:
                target = self.service_map[name]
                if target not in execution_plan:
                    execution_plan.append(target)
        
        # Limitar niveles si no es deep scan
        return execution_plan if deep_scan else execution_plan[:4]

    def enrich_book(self, lib, enterprise_id, db_ranking, deep_scan=False, has_file=False):
        """Orquesta el enriquecimiento de un solo libro."""
        isbn = lib.get('codigo') or lib.get('isbn', '')
        libro_nombre = lib.get('nombre', 'Sin título')
        
        execution_plan = self.build_execution_plan(lib, enterprise_id, db_ranking, deep_scan)
        
        api_data = {}
        success = False
        
        logger.info(f"  → Ejecutando plan ({len(execution_plan)} niveles) para: {libro_nombre}")
        
        for idx, (s_name, s_module, s_target) in enumerate(execution_plan):
            if self._is_metadata_complete(api_data):
                break
            
            try:
                # Import dinámico
                mod = __import__(s_module, fromlist=[s_target])
                target = getattr(mod, s_target)
                
                if isinstance(target, type): 
                    s_success, s_data = target().get_info(isbn)
                else: 
                    s_success, s_data = target(isbn)
                
                if s_success and isinstance(s_data, dict):
                    success = True
                    self._merge_data(api_data, s_data, s_name)
            except Exception as e:
                logger.warning(f"    [X] Error en {s_name}: {e}")

        # Procesar descarga de ebook si no tiene
        if not has_file:
            self._handle_ebook_download(lib['id'], api_data, enterprise_id)

        return success, api_data

    def _is_metadata_complete(self, data):
        """Verifica si ya tenemos los campos críticos para detener la búsqueda."""
        critical = ['cover_url', 'descripcion', 'temas', 'paginas', 'editorial']
        for field in critical:
            val = data.get(field)
            if not val or val == 'null' or (isinstance(val, list) and not val):
                return False
        return True

    def _merge_data(self, target, source, service_name):
        """Fusiona datos nuevos en el diccionario de resultados y actualiza eficiencia."""
        field_map = {
            'cover_url': 'Portada', 
            'descripcion': 'Descripción', 
            'temas': 'Temas (Género)', 
            'paginas': 'Número de Páginas', 
            'editorial': 'Editorial'
        }
        added = []
        placeholders = ["Desconocido", "Unknown", "null", "None", "n/a", "N/A"]
        
        for k, v in source.items():
            # Solo agregar si el valor es real, no está en target, o target tiene un placeholder
            val_str = str(v).strip() if v else ""
            target_val = target.get(k)
            target_is_placeholder = target_val in placeholders or not target_val
            
            if v and val_str not in placeholders:
                if target_is_placeholder:
                    target[k] = v
                    if k in field_map:
                        added.append(field_map[k])
        
        if added or 'ebook_access' in source:
            ebook_hit = 1 if source.get('ebook_access') else 0
            self.efficiency_mgr.update_score(service_name, len(added), ebook_hit)
            if added:
                logger.info(f"    [OK] {service_name} aportó: {', '.join(added)}")

    def _handle_ebook_download(self, book_id, api_data, enterprise_id):
        """Gestiona la descarga y guardado del archivo digital."""
        ebook_access = api_data.get('ebook_access')
        if not ebook_access or not ebook_access.get('url'):
            return

        url = ebook_access['url']
        logger.info(f"    [>] Descargando ebook: {url[:50]}...")
        
        content, mime, filename = library_api_service.download_ebook_content(url)
        if content:
            try:
                self.cursor.execute("""
                    INSERT INTO stk_archivos_digitales (enterprise_id, articulo_id, contenido, formato, nombre_archivo)
                    VALUES (%s, %s, %s, %s, %s)
                """, (enterprise_id, book_id, content, mime.split('/')[-1][:10], sanitize_filename(filename)))
                api_data['archivo_local'] = True
                api_data['archivo_id'] = self.cursor.lastrowid
                logger.info(f"    [OK] Ebook guardado: {filename}")
            except Exception as e:
                logger.warning(f"    [!] Error guardando BLOB: {e}")

    def update_book_record(self, lib, metadata, api_data):
        """Persiste los cambios en la tabla stk_articulos."""
        # Mapeo de Géneros
        GENRE_MAP = {
            'Computer software': 'Informática', 'Religion': 'Religión', 'History': 'Historia',
            'Fiction': 'Ficción', 'Science': 'Ciencia', 'Social Science': 'Ciencias Sociales',
            'Art': 'Arte'
        }
        
        if api_data.get('temas') and len(api_data['temas']) > 0:
            metadata['temas'] = api_data['temas']
            raw_genre = api_data['temas'][0]
            clean_genre = raw_genre.split('(')[0].strip()
            metadata['genero'] = GENRE_MAP.get(clean_genre, clean_genre)

        for field in ['descripcion', 'cover_url', 'identifiers', 'paginas', 'editorial']:
            if api_data.get(field):
                metadata[field] = api_data.get(field)

        if api_data.get('titulo') and (not lib['nombre'] or 'Biblia' in api_data.get('titulo')):
            lib['nombre'] = api_data.get('titulo')
        
        if api_data.get('autor') or api_data.get('author'):
            metadata['autor'] = api_data.get('autor') or api_data.get('author')
            
        if api_data.get('ebook_url'):
            metadata['ebook_url'] = api_data.get('ebook_url')

        # Move status and metadata fields into the JSON and use the physical columns (marca, modelo)
        metadata['api_checked'] = 2
        metadata['lengua'] = str(api_data.get('lengua') or metadata.get('lengua', 'und'))[:3].lower()
        metadata['origen'] = "Local" if metadata['lengua'] == "spa" else "Importado"

        self.cursor.execute("""
            UPDATE stk_articulos 
            SET nombre = %s, marca = %s, modelo = %s, metadata_json = %s 
            WHERE id = %s
        """, (
            lib['nombre'], 
            metadata.get('editorial', lib.get('marca')), 
            metadata.get('autor', lib.get('modelo')), 
            json.dumps(metadata), lib['id']
        ))

