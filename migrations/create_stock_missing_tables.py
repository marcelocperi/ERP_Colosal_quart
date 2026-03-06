import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate():
    with get_db_cursor() as cursor:
        
        logger.info("Creando tabla stk_impresoras_config...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_impresoras_config (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                nombre VARCHAR(100) NOT NULL,
                marca VARCHAR(50) DEFAULT 'Systel',
                modelo VARCHAR(50),
                ancho_mm INT DEFAULT 58,
                alto_mm INT DEFAULT 40,
                es_predeterminada TINYINT(1) DEFAULT 0,
                activo TINYINT(1) DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        logger.info("Creando tabla stk_numeros_serie...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_numeros_serie (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                articulo_id INT NOT NULL,
                numero_serie VARCHAR(150) NOT NULL,
                estado VARCHAR(50) DEFAULT 'DISPONIBLE',
                origen VARCHAR(50),
                tercero_id INT DEFAULT NULL,
                ubicacion_id INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_serie_art_ent (enterprise_id, articulo_id, numero_serie)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        logger.info("Creando tabla stk_series_counter...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_series_counter (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                articulo_id INT NOT NULL,
                ultimo_correlativo INT NOT NULL DEFAULT 0,
                prefijo VARCHAR(20) DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_counter_ent_art (enterprise_id, articulo_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

    logger.info("Migración completada con éxito.")

if __name__ == '__main__':
    migrate()
