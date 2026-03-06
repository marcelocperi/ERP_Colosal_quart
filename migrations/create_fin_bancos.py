"""
Migración: Crear tabla fin_bancos para el ABM de Entidades Bancarias.

Integra el catálogo oficial del BCRA con el esquema multi-tenant del ERP.

Esquema de enterprise_id:
  - enterprise_id = 0  → datos maestros globales (importados del BCRA)
  - enterprise_id = N  → bancos creados manualmente por la empresa N

Ejecutar: python migrations/create_fin_bancos.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_cursor
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def up():
    """Aplica la migración."""
    with get_db_cursor() as cursor:
        # ── Crear tabla fin_bancos ──────────────────────────────────────────────
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fin_bancos (
                id            INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL DEFAULT 0
                              COMMENT '0 = dato maestro global (BCRA); N = empresa propietaria',
                bcra_id       INT NULL
                              COMMENT 'ID de entidad en el registro del BCRA (cdEntidad)',
                nombre        VARCHAR(200) NOT NULL,
                tipo          VARCHAR(100) NULL
                              COMMENT 'Tipo: Banco Comercial, Caja de Crédito, etc.',
                cuit          VARCHAR(15)  NULL,
                bic           VARCHAR(20)  NULL
                              COMMENT 'Código BIC/SWIFT internacional',
                direccion     VARCHAR(300) NULL,
                telefono      VARCHAR(50)  NULL,
                web           VARCHAR(200) NULL,
                activo        TINYINT(1)   NOT NULL DEFAULT 1,
                origen        ENUM('MANUAL','BCRA') NOT NULL DEFAULT 'MANUAL'
                              COMMENT 'MANUAL = alta por usuario; BCRA = importado de la API',
                created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                -- Evitar duplicados por entidad BCRA
                UNIQUE KEY uq_bcra_id (bcra_id),

                -- Índices de consulta frecuente
                INDEX idx_enterprise   (enterprise_id),
                INDEX idx_nombre       (nombre(100)),
                INDEX idx_activo       (activo),
                INDEX idx_origen       (origen)
            ) ENGINE=InnoDB
              DEFAULT CHARSET=utf8mb4
              COLLATE=utf8mb4_unicode_ci
              COMMENT='Catálogo de entidades bancarias. Origen: BCRA API o alta manual.'
        """)
        logger.info("✅  Tabla fin_bancos creada (o ya existía).")


def down():
    """Revierte la migración (¡destructivo!)."""
    confirm = input("⚠️  ¿Eliminar la tabla fin_bancos? Escriba 'CONFIRMAR' para continuar: ")
    if confirm.strip() == 'CONFIRMAR':
        with get_db_cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS fin_bancos")
        logger.info("🗑️   Tabla fin_bancos eliminada.")
    else:
        logger.info("Operación cancelada.")


if __name__ == '__main__':
    action = sys.argv[1] if len(sys.argv) > 1 else 'up'
    if action == 'down':
        down()
    else:
        up()
    logger.info("Migración completada.")
