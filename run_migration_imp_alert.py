
import logging
import os
import sys

# Añadir el path raíz para importaciones de módulos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def migrate_alert_status():
    """Migración: Agregar control de envío de alertas de demora."""
    
    with database.get_db_cursor() as cursor:
        try:
            logger.info("Agregando campo alerta_demora_enviada a imp_despachos...")
            
            # Verificamos si la columna ya existe
            cursor.execute("SHOW COLUMNS FROM imp_despachos")
            columns = [col[0] for col in cursor.fetchall()]
            
            if 'alerta_demora_enviada' not in columns:
                cursor.execute("ALTER TABLE imp_despachos ADD COLUMN alerta_demora_enviada TINYINT DEFAULT 0 AFTER fecha_devolucion_contenedor")
                logger.info("Columna 'alerta_demora_enviada' creada.")
            else:
                logger.info("Columna 'alerta_demora_enviada' ya existe.")
            
            logger.info("Migración completada con éxito.")
            
        except Exception as e:
            logger.error(f"Error en migración: {e}")
            raise e

if __name__ == "__main__":
    migrate_alert_status()
