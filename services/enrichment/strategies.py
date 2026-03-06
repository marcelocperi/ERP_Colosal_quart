
"""
Módulo que define estrategias para seleccionar qué libros necesitan enriquecimiento.
"""
from database import DB_CONFIG
import logging

logger = logging.getLogger('enrich_strategy')

class EnrichmentStrategy:
    CONSERVATIVE = 'conservative' # Solo faltantes críticos
    EXHAUSTIVE = 'exhaustive'     # Revisar todo el catálogo
    SMART = 'smart'               # Faltantes relevantes (Temas, págs, portadas)

def get_books_to_process_query(strategy, enterprise_id, limit, deep_scan=False):
    """
    Construye la query SQL para obtener libros pendientes según estrategia.
    Retorna (sql, params).
    """
    if not deep_scan:
        # Modo rápido: Solo libros nuevos o nunca revisados
        query = """
            SELECT l.*
            FROM stk_articulos l
            WHERE l.enterprise_id = %s 
              AND (JSON_EXTRACT(l.metadata_json, '$.api_checked') IS NULL OR JSON_EXTRACT(l.metadata_json, '$.api_checked') = 0) 
              AND l.codigo IS NOT NULL
            LIMIT %s
        """
        return query, (enterprise_id, limit)

    # Modo Deep Scan: Filtrado avanzado
    filter_clause = "1=1"
    
    if strategy == EnrichmentStrategy.EXHAUSTIVE:
        # B: Exhaustiva - Procesa TODO el catálogo sin filtros
        filter_clause = "1=1"
        logger.info("  [Estrategia: EXHAUSTIVA] Procesando catálogo completo.")
        
    elif strategy == EnrichmentStrategy.SMART:
        # C: Inteligente - Procesa si falta CUALQUIER dato relevante
        filter_clause = """
            (JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.cover_url')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.cover_url')) = '') OR
            (JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.descripcion')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.descripcion')) = '') OR
            (JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.paginas')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.paginas')) = 0) OR
            (JSON_EXTRACT(l.metadata_json, '$.temas') IS NULL OR JSON_LENGTH(JSON_EXTRACT(l.metadata_json, '$.temas')) = 0)
        """
        logger.info("  [Estrategia: INTELIGENTE] Buscando cualquier campo incompleto.")
        
    else:
        # A: Conservadora (Default) - Solo si faltan datos críticos (Portada O Descripción)
        filter_clause = """
            (JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.cover_url')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.cover_url')) = '') OR
            (JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.descripcion')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(l.metadata_json, '$.descripcion')) = '')
        """
        logger.info("  [Estrategia: CONSERVADORA] Solo datos críticos faltantes.")

    query = f"""
        SELECT l.* 
        FROM stk_articulos l
        WHERE l.enterprise_id = %s AND l.codigo IS NOT NULL
        AND ({filter_clause})
        LIMIT %s
    """
    return query, (enterprise_id, limit)
