
"""
Módulo dedicado a actualizar y consultar la eficiencia de los servicios de enriquecimiento.
(Aprendizaje automático sobre qué fuente es mejor).
"""
import logging
from database import get_db_pool

logger = logging.getLogger('efficiency_service')

class EfficiencyManager:
    """Maneja el registro y consulta de eficiencia de APIs."""

    def __init__(self, conn=None):
        self.conn = conn
        self.cursor = conn.cursor() if conn else None

    def get_service_ranking(self):
        """Devuelve una lista ordenada de servicios por eficiencia."""
        if not self.cursor: return []
        try:
            self.cursor.execute("SELECT service_name FROM service_efficiency ORDER BY fields_provided DESC, hits_count DESC")
            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            logger.warning(f"Error cargando ranking de eficiencia: {e}")
            return []

    def update_score(self, service_name, fields_count, ebook_found=0):
        """Registra un éxito para un servicio (UPSERT)."""
        if not self.cursor: return
        query = """
            INSERT INTO service_efficiency (service_name, hits_count, fields_provided, ebooks_provided)
            VALUES (%s, 1, %s, %s)
            ON DUPLICATE KEY UPDATE 
                hits_count = hits_count + 1, 
                fields_provided = fields_provided + %s, 
                ebooks_provided = ebooks_provided + %s
        """
        try:
            self.cursor.execute(query, (service_name, fields_count, ebook_found, fields_count, ebook_found))
            # No commit explícito aquí si es parte de una transacción mayor
        except Exception as e:
            logger.warning(f"Error actualizando score de {service_name}: {e}")

    def rotate_learning_cycle(self):
        """Reinicia el aprendizaje cada ciertos ciclos para adaptarse a nuevas fuentes."""
        if not self.cursor: return
        try:
            # Incrementar contador global
            self.cursor.execute("UPDATE sys_enrichment_counters SET processed_since_reset = processed_since_reset + 1 WHERE id = 1")
            
            # Verificar umbral (300 libros)
            self.cursor.execute("SELECT processed_since_reset FROM sys_enrichment_counters WHERE id = 1")
            row = self.cursor.fetchone()
            
            if row and row[0] >= 300:
                logger.info("  ♻️ [APRENDIZAJE] Ciclo completado. Reiniciando estadísticas de eficiencia...")
                self.cursor.execute("TRUNCATE TABLE service_efficiency")
                self.cursor.execute("UPDATE sys_enrichment_counters SET processed_since_reset = 0 WHERE id = 1")
                self.conn.commit()
                logger.info("  >> Estadísticas truncadas para nueva evaluación.")
        except Exception as e:
            logger.warning(f"Error en ciclo de aprendizaje: {e}")
