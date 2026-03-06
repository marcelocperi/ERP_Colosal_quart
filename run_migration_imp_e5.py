
import logging
import os
import sys

# Añadir el path raíz para importaciones de módulos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def migrate_stage_5():
    """Migración Etapa 5: Gestión de Puerto y Demoras."""
    
    with database.get_db_cursor() as cursor:
        try:
            logger.info("Agregando campos de gestión de puerto a imp_despachos...")
            
            # Verificamos si las columnas ya existen
            cursor.execute("SHOW COLUMNS FROM imp_despachos")
            columns = [col[0] for col in cursor.fetchall()]
            
            new_columns = [
                ("dias_libres_puerto", "INT DEFAULT 0 AFTER peso_kg"),
                ("costo_demora_diario_usd", "DECIMAL(12,2) DEFAULT 0.00 AFTER dias_libres_puerto"),
                ("fecha_devolucion_contenedor", "DATE NULL AFTER costo_demora_diario_usd")
            ]
            
            for col_name, col_def in new_columns:
                if col_name not in columns:
                    sql = f"ALTER TABLE imp_despachos ADD COLUMN {col_name} {col_def}"
                    cursor.execute(sql)
                    logger.info(f"Columna '{col_name}' creada.")
                else:
                    logger.info(f"Columna '{col_name}' ya existe.")
            
            logger.info("Migración Etapa 5 completada con éxito.")
            
        except Exception as e:
            logger.error(f"Error en migración: {e}")
            raise e

if __name__ == "__main__":
    migrate_stage_5()
