import database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_stage_4():
    """Migración Etapa 4: Seguimiento Logístico."""
    
    with database.get_db_cursor() as cursor:
        try:
            logger.info("Agregando campos logísticos a imp_despachos...")
            
            # Verificamos si las columnas ya existen para evitar errores
            cursor.execute("SHOW COLUMNS FROM imp_despachos")
            columns = [col[0] for col in cursor.fetchall()]
            
            new_columns = [
                ("transportista", "VARCHAR(100) NULL AFTER canal"),
                ("guia_bl_tracking", "VARCHAR(100) NULL AFTER transportista"),
                ("vessel_mmsi", "VARCHAR(30) NULL AFTER guia_bl_tracking"),
                ("vessel_name", "VARCHAR(100) NULL AFTER vessel_mmsi"),
                ("fecha_embarque", "DATE NULL AFTER vessel_name"),
                ("fecha_arribo_estimada", "DATE NULL AFTER fecha_embarque"),
                ("fecha_arribo_real", "DATE NULL AFTER fecha_arribo_estimada"),
                ("puerto_embarque", "VARCHAR(100) NULL AFTER fecha_arribo_real"),
                ("puerto_destino", "VARCHAR(100) NULL AFTER puerto_embarque"),
                ("bultos", "INT NULL AFTER puerto_destino"),
                ("peso_kg", "DECIMAL(12,2) NULL AFTER bultos")
            ]
            
            for col_name, col_def in new_columns:
                if col_name not in columns:
                    sql = f"ALTER TABLE imp_despachos ADD COLUMN {col_name} {col_def}"
                    cursor.execute(sql)
                    logger.info(f"Columna '{col_name}' creada.")
                else:
                    logger.info(f"Columna '{col_name}' ya existe.")
            
            logger.info("Migración Etapa 4 completada con éxito.")
            
        except Exception as e:
            logger.error(f"Error en migración: {e}")
            raise e

if __name__ == "__main__":
    migrate_stage_4()
