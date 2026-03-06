"""
Migración: Agregar tipo_entidad y numero_cuenta a fin_bancos.

- tipo_entidad ENUM('CBU','CVU'): identifica el tipo de instrumento.
  CBU  → entidades bancarias (BCRA). Cuenta analítica: 1.1.02.XXX
  CVU  → billeteras virtuales (PSP). Cuenta analítica: 1.1.03.XXX

- numero_cuenta VARCHAR(22): número completo del CBU/CVU (22 dígitos).
  Para CVU: los últimos 3 dígitos determinan el código de cuenta (1.1.03.XXX).

Ejecutar: python migrations/alter_fin_bancos_add_tipo_entidad.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_cursor
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def col_exists(cursor, table, col):
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """, (table, col))
    return cursor.fetchone()[0] > 0


def up():
    with get_db_cursor() as cursor:

        # 1. tipo_entidad  ENUM('CBU','CVU')  DEFAULT 'CBU'
        if not col_exists(cursor, 'fin_bancos', 'tipo_entidad'):
            cursor.execute("""
                ALTER TABLE fin_bancos
                ADD COLUMN tipo_entidad ENUM('CBU','CVU') NOT NULL DEFAULT 'CBU'
                    COMMENT 'CBU = banco tradicional; CVU = billetera virtual PSP'
                AFTER bcra_id
            """)
            logger.info("✅  Columna tipo_entidad agregada.")
        else:
            logger.info("ℹ️   tipo_entidad ya existe.")

        # 2. numero_cuenta  VARCHAR(22)
        if not col_exists(cursor, 'fin_bancos', 'numero_cuenta'):
            cursor.execute("""
                ALTER TABLE fin_bancos
                ADD COLUMN numero_cuenta VARCHAR(22) NULL
                    COMMENT 'Número completo del CBU/CVU (22 dígitos)'
                AFTER tipo_entidad
            """)
            logger.info("✅  Columna numero_cuenta agregada.")
        else:
            logger.info("ℹ️   numero_cuenta ya existe.")

        # 3. Actualizar registros existentes: marcar como CBU los que no tienen tipo
        cursor.execute("""
            UPDATE fin_bancos SET tipo_entidad = 'CBU'
            WHERE tipo_entidad IS NULL OR tipo_entidad = ''
        """)

        # 4. Asegurar que la cuenta padre 1.1.03 Billeteras Virtuales exista en enterprise_id=0
        cursor.execute("""
            SELECT id FROM cont_plan_cuentas
            WHERE codigo = '1.1.03' AND enterprise_id = 0
        """)
        if not cursor.fetchone():
            # Buscar padre 1.1
            cursor.execute("""
                SELECT id FROM cont_plan_cuentas
                WHERE codigo = '1.1' AND (enterprise_id = 0 OR enterprise_id IS NULL)
                LIMIT 1
            """)
            row = cursor.fetchone()
            padre_id = row[0] if row else None
            cursor.execute("""
                INSERT INTO cont_plan_cuentas
                    (enterprise_id, codigo, nombre, tipo, imputable, padre_id, nivel, es_analitica)
                VALUES (0, '1.1.03', 'Billeteras Virtuales', 'ACTIVO', 0, %s, 3, 0)
            """, (padre_id,))
            logger.info("✅  Cuenta padre 1.1.03 Billeteras Virtuales creada.")
        else:
            logger.info("ℹ️   Cuenta 1.1.03 ya existe.")

    logger.info("Migración completada.")


if __name__ == '__main__':
    up()
