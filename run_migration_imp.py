#!/usr/bin/env python3
"""
Migración Etapa 1 — Módulo de Importaciones
Ejecutar desde el directorio raíz del proyecto: python run_migration_imp.py
"""
import sys
import os

# Asegurar que el path al proyecto esté disponible
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from database import get_db_cursor
    print("[OK] database importada correctamente")
except ImportError as e:
    print(f"[ERROR] No se pudo importar database: {e}")
    sys.exit(1)

SQL_STEPS = [
    # 1. Columnas en erp_terceros
    ("erp_terceros: es_proveedor_extranjero",
     "ALTER TABLE erp_terceros ADD COLUMN IF NOT EXISTS es_proveedor_extranjero TINYINT(1) DEFAULT 0"),
    ("erp_terceros: pais_origen",
     "ALTER TABLE erp_terceros ADD COLUMN IF NOT EXISTS pais_origen VARCHAR(100) DEFAULT NULL"),
    ("erp_terceros: codigo_pais_iso",
     "ALTER TABLE erp_terceros ADD COLUMN IF NOT EXISTS codigo_pais_iso CHAR(2) DEFAULT NULL"),
    ("erp_terceros: identificador_fiscal",
     "ALTER TABLE erp_terceros ADD COLUMN IF NOT EXISTS identificador_fiscal VARCHAR(60) DEFAULT NULL"),
    ("erp_terceros: codigo_swift",
     "ALTER TABLE erp_terceros ADD COLUMN IF NOT EXISTS codigo_swift VARCHAR(20) DEFAULT NULL"),
    ("erp_terceros: moneda_operacion",
     "ALTER TABLE erp_terceros ADD COLUMN IF NOT EXISTS moneda_operacion CHAR(3) DEFAULT 'ARS'"),
    ("erp_terceros: web",
     "ALTER TABLE erp_terceros ADD COLUMN IF NOT EXISTS web VARCHAR(200) DEFAULT NULL"),

    # 2. Tabla de tipos de cambio
    ("fin_tipos_cambio: CREATE",
     """CREATE TABLE IF NOT EXISTS fin_tipos_cambio (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id   INT NOT NULL DEFAULT 0,
        fecha           DATE NOT NULL,
        moneda          CHAR(3) NOT NULL,
        tipo            VARCHAR(30) NOT NULL,
        valor           DECIMAL(18,6) NOT NULL,
        fuente          VARCHAR(50) DEFAULT 'BCRA',
        user_id         INT DEFAULT NULL,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_tipo_cambio (enterprise_id, fecha, moneda, tipo)
     )"""),

    # 2b. Seed tipos de cambio
    ("fin_tipos_cambio: SEED USD Oficial",
     "INSERT IGNORE INTO fin_tipos_cambio (enterprise_id, fecha, moneda, tipo, valor, fuente) VALUES (0, CURDATE(), 'USD', 'OFICIAL_VENDEDOR', 1050.00, 'SEED')"),
    ("fin_tipos_cambio: SEED USD Comprador",
     "INSERT IGNORE INTO fin_tipos_cambio (enterprise_id, fecha, moneda, tipo, valor, fuente) VALUES (0, CURDATE(), 'USD', 'OFICIAL_COMPRADOR', 1040.00, 'SEED')"),
    ("fin_tipos_cambio: SEED USD MEP",
     "INSERT IGNORE INTO fin_tipos_cambio (enterprise_id, fecha, moneda, tipo, valor, fuente) VALUES (0, CURDATE(), 'USD', 'MEP', 1200.00, 'SEED')"),
    ("fin_tipos_cambio: SEED EUR",
     "INSERT IGNORE INTO fin_tipos_cambio (enterprise_id, fecha, moneda, tipo, valor, fuente) VALUES (0, CURDATE(), 'EUR', 'OFICIAL_VENDEDOR', 1130.00, 'SEED')"),
    ("fin_tipos_cambio: SEED BRL",
     "INSERT IGNORE INTO fin_tipos_cambio (enterprise_id, fecha, moneda, tipo, valor, fuente) VALUES (0, CURDATE(), 'BRL', 'OFICIAL_VENDEDOR', 200.00, 'SEED')"),

    # 3. Columnas en cmp_ordenes_compra
    ("cmp_ordenes_compra: es_importacion",
     "ALTER TABLE cmp_ordenes_compra ADD COLUMN IF NOT EXISTS es_importacion TINYINT(1) DEFAULT 0"),
    ("cmp_ordenes_compra: moneda",
     "ALTER TABLE cmp_ordenes_compra ADD COLUMN IF NOT EXISTS moneda CHAR(3) DEFAULT 'ARS'"),
    ("cmp_ordenes_compra: tipo_cambio_valor",
     "ALTER TABLE cmp_ordenes_compra ADD COLUMN IF NOT EXISTS tipo_cambio_valor DECIMAL(18,6) DEFAULT NULL"),
    ("cmp_ordenes_compra: incoterm",
     "ALTER TABLE cmp_ordenes_compra ADD COLUMN IF NOT EXISTS incoterm VARCHAR(10) DEFAULT NULL"),
    ("cmp_ordenes_compra: pais_origen",
     "ALTER TABLE cmp_ordenes_compra ADD COLUMN IF NOT EXISTS pais_origen VARCHAR(100) DEFAULT NULL"),
    ("cmp_ordenes_compra: puerto_embarque",
     "ALTER TABLE cmp_ordenes_compra ADD COLUMN IF NOT EXISTS puerto_embarque VARCHAR(150) DEFAULT NULL"),
    ("cmp_ordenes_compra: puerto_destino",
     "ALTER TABLE cmp_ordenes_compra ADD COLUMN IF NOT EXISTS puerto_destino VARCHAR(150) DEFAULT NULL"),
    ("cmp_ordenes_compra: total_estimado_usd",
     "ALTER TABLE cmp_ordenes_compra ADD COLUMN IF NOT EXISTS total_estimado_usd DECIMAL(18,2) DEFAULT NULL"),

    # 4. Tabla documentos de importación
    ("imp_documentos: CREATE",
     """CREATE TABLE IF NOT EXISTS imp_documentos (
        id               INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id    INT NOT NULL,
        orden_compra_id  INT DEFAULT NULL,
        tipo_documento   VARCHAR(40) NOT NULL,
        numero_documento VARCHAR(80) DEFAULT NULL,
        fecha_documento  DATE DEFAULT NULL,
        fecha_vencimiento DATE DEFAULT NULL,
        monto            DECIMAL(18,2) DEFAULT NULL,
        moneda           CHAR(3) DEFAULT 'USD',
        proveedor_id     INT DEFAULT NULL,
        descripcion      TEXT DEFAULT NULL,
        archivo_path     VARCHAR(300) DEFAULT NULL,
        estado           VARCHAR(30) DEFAULT 'PENDIENTE',
        user_id          INT DEFAULT NULL,
        created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
     )"""),

    # 5. Tabla cargos de importación
    ("imp_cargos: CREATE",
     """CREATE TABLE IF NOT EXISTS imp_cargos (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id   INT NOT NULL,
        orden_compra_id INT NOT NULL,
        tipo_cargo      VARCHAR(50) NOT NULL,
        descripcion     VARCHAR(200) DEFAULT NULL,
        proveedor_id    INT DEFAULT NULL,
        monto_orig      DECIMAL(18,2) DEFAULT NULL,
        moneda_orig     CHAR(3) DEFAULT 'USD',
        tipo_cambio     DECIMAL(18,6) DEFAULT NULL,
        monto_ars       DECIMAL(18,2) DEFAULT NULL,
        comprobante_id  INT DEFAULT NULL,
        fecha           DATE DEFAULT NULL,
        user_id         INT DEFAULT NULL,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     )"""),

    # 6. Índices
    ("idx fin_tipos_cambio",
     "CREATE INDEX IF NOT EXISTS idx_fin_tc_fecha ON fin_tipos_cambio (fecha, moneda)"),
    ("idx imp_documentos",
     "CREATE INDEX IF NOT EXISTS idx_imp_docs_orden ON imp_documentos (orden_compra_id)"),
    ("idx imp_cargos",
     "CREATE INDEX IF NOT EXISTS idx_imp_cargos ON imp_cargos (orden_compra_id)"),
]


def run_migration():
    ok = 0
    warn = 0
    err = 0

    print("\n" + "="*60)
    print("  MIGRACIÓN ETAPA 1 — Módulo de Importaciones")
    print("="*60)

    with get_db_cursor() as cursor:
        for label, sql in SQL_STEPS:
            try:
                cursor.execute(sql)
                print(f"  [✓] {label}")
                ok += 1
            except Exception as e:
                msg = str(e)
                # Si el error es "columna ya existe" o "tabla ya existe", es OK
                if "Duplicate column" in msg or "already exists" in msg or "Can't DROP" in msg:
                    print(f"  [~] {label} — ya existía (omitido)")
                    warn += 1
                else:
                    print(f"  [✗] {label}")
                    print(f"       Error: {msg}")
                    err += 1

    print("\n" + "-"*60)
    print(f"  Resultado: {ok} OK  |  {warn} omitidos  |  {err} errores")
    print("="*60 + "\n")

    return err == 0


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
