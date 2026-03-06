"""
Migración: Agregar campos cbu y cuenta_contable_id a fin_bancos.

- codigo_cbu (3 dígitos): código del banco en el CBU (posiciones 1-3)
  Ej: 017 = BBVA,  011 = Nación,  014 = Provincia, etc.
- cuenta_contable_id: FK a cont_plan_cuentas (cuenta analítica 1.1.02.XXX)

Ejecutar: python migrations/alter_fin_bancos_add_cbu.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_cursor
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def column_exists(cursor, table, column):
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (table, column))
    return cursor.fetchone()[0] > 0


def up():
    with get_db_cursor() as cursor:
        # 1. codigo_cbu: 3 dígitos del CBU que identifican al banco
        if not column_exists(cursor, 'fin_bancos', 'codigo_cbu'):
            cursor.execute("""
                ALTER TABLE fin_bancos
                ADD COLUMN codigo_cbu CHAR(3) NULL
                    COMMENT 'Bytes 1-3 del CBU: identificador del banco (ej: 017=BBVA)'
                AFTER bcra_id
            """)
            logger.info("✅  Columna codigo_cbu agregada a fin_bancos.")
        else:
            logger.info("ℹ️   codigo_cbu ya existe.")

        # 2. cuenta_contable_id: FK a cont_plan_cuentas
        if not column_exists(cursor, 'fin_bancos', 'cuenta_contable_id'):
            cursor.execute("""
                ALTER TABLE fin_bancos
                ADD COLUMN cuenta_contable_id INT NULL
                    COMMENT 'FK → cont_plan_cuentas.id (cuenta analítica 1.1.02.XXX)'
                AFTER codigo_cbu
            """)
            logger.info("✅  Columna cuenta_contable_id agregada a fin_bancos.")
        else:
            logger.info("ℹ️   cuenta_contable_id ya existe.")

        # 3. Índice en codigo_cbu para búsquedas rápidas
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'fin_bancos' AND INDEX_NAME = 'idx_codigo_cbu'
        """)
        if cursor.fetchone()[0] == 0:
            cursor.execute("ALTER TABLE fin_bancos ADD INDEX idx_codigo_cbu (codigo_cbu)")
            logger.info("✅  Índice idx_codigo_cbu creado.")

    logger.info("Migración completada.")


if __name__ == '__main__':
    up()
