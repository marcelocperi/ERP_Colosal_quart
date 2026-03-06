ya"""
Migration: Tax Engine - Tablas parametrizables por empresa y operación de negocio.

Diseño:
  - tax_impuestos       : Catálogo maestro de impuestos (global)
  - tax_alicuotas       : Alícuotas por empresa, impuesto y vigencia
  - tax_reglas          : Reglas que definen qué impuestos aplican según
                          operación (COMPRAS/VENTAS/COBRANZAS/PAGOS),
                          perfil fiscal del tercero y condición IIBB
  - tax_reglas_iibb     : Detalle de jurisdicciones IIBB por condición

Herencia: enterprise_id=0 es la plantilla global. Si una empresa no tiene
          reglas propias, el engine usa las del enterprise_id=0.

Ejecutar: python migrations/create_tax_engine_tables.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

DDL = [

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Catálogo de impuestos (global, compartido)
    # ─────────────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS tax_impuestos (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        codigo          VARCHAR(30)  NOT NULL UNIQUE,
        nombre          VARCHAR(100) NOT NULL,
        tipo            ENUM('IVA','IIBB','PERCEPCION_IVA','RETENCION','OTRO') NOT NULL,
        descripcion     TEXT,
        activo          TINYINT(1) DEFAULT 1,
        orden_display   INT DEFAULT 0,
        created_at      DATETIME DEFAULT NOW()
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='Catálogo maestro de tipos de impuesto. Global, compartido entre empresas.';
    """,

    # ─────────────────────────────────────────────────────────────────────────
    # 2. Alícuotas por empresa y vigencia temporal
    # ─────────────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS tax_alicuotas (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id   INT NOT NULL COMMENT '0 = plantilla global',
        impuesto_id     INT NOT NULL,
        alicuota        DECIMAL(8,4) NOT NULL,
        base_calculo    ENUM('NETO_GRAVADO','TOTAL','NETO_MAS_IVA') DEFAULT 'NETO_GRAVADO',
        vigencia_desde  DATE NOT NULL DEFAULT '2000-01-01',
        vigencia_hasta  DATE          DEFAULT NULL COMMENT 'NULL = vigente indefinidamente',
        activo          TINYINT(1) DEFAULT 1,
        observaciones   VARCHAR(255),
        updated_at      DATETIME DEFAULT NOW() ON UPDATE NOW(),
        FOREIGN KEY (impuesto_id) REFERENCES tax_impuestos(id),
        INDEX idx_ent_imp   (enterprise_id, impuesto_id),
        INDEX idx_vigencia  (vigencia_desde, vigencia_hasta)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='Alícuotas configurables por empresa. enterprise_id=0 es la plantilla global.';
    """,

    # ─────────────────────────────────────────────────────────────────────────
    # 3. Reglas fiscales: operación × perfil fiscal → impuestos aplicables
    #
    #    operacion:        COMPRAS | VENTAS | COBRANZAS | PAGOS
    #    tipo_responsable: RI | MONOTRIBUTO | EXENTO | NO_RESPONSABLE | etc.
    #    condicion_iibb:   ARBA | AGIP | AMBOS | CONVENIO_MULTILATERAL | EXENTO | ''
    #
    #    Una regla dice: "En COMPRAS, para un proveedor RI con IIBB ARBA,
    #                     aplica IVA_21, IVA_10_5, IVA_27, IIBB_ARBA"
    # ─────────────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS tax_reglas (
        id                  INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id       INT NOT NULL COMMENT '0 = plantilla global',
        operacion           ENUM('COMPRAS','VENTAS','COBRANZAS','PAGOS') NOT NULL,
        tipo_responsable    VARCHAR(50) NOT NULL
                            COMMENT 'RI, MONOTRIBUTO, EXENTO, NO_RESPONSABLE, CONSUMIDOR_FINAL, * (todos)',
        condicion_iibb      VARCHAR(50) NOT NULL DEFAULT '*'
                            COMMENT 'ARBA, AGIP, AMBOS, CONVENIO_MULTILATERAL, EXENTO, * (cualquiera)',
        exencion_iibb       VARCHAR(20) NOT NULL DEFAULT '*'
                            COMMENT '* = cualquiera | EXENTO = solo si el padron informa exento | NO_EXENTO = solo si NO es exento',
        impuesto_id         INT NOT NULL,
        aplica              TINYINT(1) DEFAULT 1 COMMENT '1=aplica, 0=excluir explicitamente',
        es_obligatorio      TINYINT(1) DEFAULT 0 COMMENT '1=siempre visible, 0=solo si hay importe',
        activo              TINYINT(1) DEFAULT 1,
        FOREIGN KEY (impuesto_id) REFERENCES tax_impuestos(id),
        UNIQUE KEY uq_regla (enterprise_id, operacion, tipo_responsable, condicion_iibb, exencion_iibb, impuesto_id),
        INDEX idx_lookup    (enterprise_id, operacion, tipo_responsable, condicion_iibb, exencion_iibb)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='Reglas fiscales por operacion de negocio, perfil del tercero, condicion IIBB y exencion.';
    """,

    # ─────────────────────────────────────────────────────────────────────────
    # 4. Detalle de jurisdicciones IIBB (para Convenio Multilateral y otros)
    # ─────────────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS tax_reglas_iibb (
        id                  INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id       INT NOT NULL COMMENT '0 = plantilla global',
        condicion_iibb      VARCHAR(50) NOT NULL,
        jurisdiccion_codigo INT          DEFAULT NULL COMMENT 'NULL = aplica a todas (CM)',
        jurisdiccion_nombre VARCHAR(100) DEFAULT NULL,
        impuesto_id         INT NOT NULL,
        alicuota_override   DECIMAL(8,4) DEFAULT NULL
                            COMMENT 'NULL = usar tax_alicuotas; valor = override específico',
        usa_padron          TINYINT(1) DEFAULT 1
                            COMMENT '1=consultar padrón ARBA/AGIP para alícuota real',
        regimen             ENUM('LOCAL','CM_GENERAL','CM_ESPECIAL') DEFAULT 'LOCAL',
        limite_cm_pct       DECIMAL(5,2) DEFAULT 100.00
                            COMMENT 'Para CM: % máximo de la alícuota normal (reglamentación: 50%)',
        coef_minimo_cm      DECIMAL(6,4) DEFAULT 0.0000
                            COMMENT 'Para CM: coeficiente mínimo para que aplique percepción (0.1000)',
        activo              TINYINT(1) DEFAULT 1,
        FOREIGN KEY (impuesto_id) REFERENCES tax_impuestos(id),
        INDEX idx_ent_cond  (enterprise_id, condicion_iibb)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='Detalle de jurisdicciones IIBB. Incluye reglas para Convenio Multilateral.';
    """,
]

# ─────────────────────────────────────────────────────────────────────────────
# DATOS SEMILLA
# ─────────────────────────────────────────────────────────────────────────────

SEED_IMPUESTOS = [
    # (codigo, nombre, tipo, descripcion, orden)
    ('IVA_21',    'IVA 21%',                         'IVA',            'Alícuota general',                            1),
    ('IVA_10_5',  'IVA 10.5%',                       'IVA',            'Alícuota reducida',                           2),
    ('IVA_27',    'IVA 27%',                          'IVA',            'Servicios públicos y diferencial',            3),
    ('PERC_IVA',  'Percepción IVA',                   'PERCEPCION_IVA', 'Percepción de IVA por agente',                4),
    ('IIBB_ARBA', 'IIBB ARBA (Bs.As.)',               'IIBB',           'Ingresos Brutos - Prov. Buenos Aires',        5),
    ('IIBB_AGIP', 'IIBB AGIP (CABA)',                 'IIBB',           'Ingresos Brutos - CABA',                      6),
    ('IIBB_CM',   'IIBB Convenio Multilateral',       'IIBB',           'Ingresos Brutos - Régimen CM (multi-jurisd)', 7),
    ('OTROS_IMP', 'Otros Impuestos',                  'OTRO',           'Impuestos internos u otros conceptos',        8),
]

SEED_ALICUOTAS = [
    # (codigo_impuesto, alicuota, base_calculo)
    ('IVA_21',    21.0000, 'NETO_GRAVADO'),
    ('IVA_10_5',  10.5000, 'NETO_GRAVADO'),
    ('IVA_27',    27.0000, 'NETO_GRAVADO'),
    ('PERC_IVA',   0.0000, 'NETO_GRAVADO'),  # Variable por empresa
    ('IIBB_ARBA',  0.0000, 'NETO_GRAVADO'),  # Variable, viene del padrón
    ('IIBB_AGIP',  0.0000, 'NETO_GRAVADO'),  # Variable, viene del padrón
    ('IIBB_CM',    0.0000, 'NETO_GRAVADO'),  # Variable, por jurisdicción
    ('OTROS_IMP',  0.0000, 'TOTAL'),
]

# Reglas por operación × tipo_responsable × condicion_iibb → [impuestos]
# '*' = comodín (aplica a cualquier valor)
SEED_REGLAS = [
    # ── COMPRAS ──────────────────────────────────────────────────────────────
    # RI: discrimina IVA + puede tener IIBB segun condicion
    ('COMPRAS', 'RI', 'ARBA',                 '*', ['IVA_21','IVA_10_5','IVA_27','PERC_IVA','IIBB_ARBA','OTROS_IMP']),
    ('COMPRAS', 'RI', 'AGIP',                 '*', ['IVA_21','IVA_10_5','IVA_27','PERC_IVA','IIBB_AGIP','OTROS_IMP']),
    ('COMPRAS', 'RI', 'AMBOS',                '*', ['IVA_21','IVA_10_5','IVA_27','PERC_IVA','IIBB_ARBA','IIBB_AGIP','OTROS_IMP']),
    ('COMPRAS', 'RI', 'CONVENIO_MULTILATERAL','*', ['IVA_21','IVA_10_5','IVA_27','PERC_IVA','IIBB_CM','OTROS_IMP']),
    ('COMPRAS', 'RI', 'CM',                   '*', ['IVA_21','IVA_10_5','IVA_27','PERC_IVA','IIBB_CM','OTROS_IMP']),
    ('COMPRAS', 'RI', '*',                    '*', ['IVA_21','IVA_10_5','IVA_27','PERC_IVA','OTROS_IMP']),
    # Monotributo: NO discrimina IVA, puede tener IIBB
    # Caso general: AGIP sin exencion → aplica IIBB_AGIP
    ('COMPRAS', 'MONOTRIBUTO', 'ARBA',                 '*',        ['IIBB_ARBA','OTROS_IMP']),
    ('COMPRAS', 'MONOTRIBUTO', 'AGIP',                 'NO_EXENTO',['IIBB_AGIP','OTROS_IMP']),
    # CABA (AGIP): profesional monotributista con exencion impositiva → sin IIBB
    # El padron AGIP informa EXENTO → no corresponde percepcion de IIBB
    ('COMPRAS', 'MONOTRIBUTO', 'AGIP',                 'EXENTO',   ['OTROS_IMP']),
    ('COMPRAS', 'MONOTRIBUTO', 'AMBOS',                'NO_EXENTO',['IIBB_ARBA','IIBB_AGIP','OTROS_IMP']),
    ('COMPRAS', 'MONOTRIBUTO', 'AMBOS',                'EXENTO',   ['IIBB_ARBA','OTROS_IMP']),  # ARBA aplica, AGIP exento
    ('COMPRAS', 'MONOTRIBUTO', 'CONVENIO_MULTILATERAL','*',        ['IIBB_CM','OTROS_IMP']),
    ('COMPRAS', 'MONOTRIBUTO', '*',                    '*',        ['OTROS_IMP']),
    ('COMPRAS', 'MONOTRIBUTISTA','*',                  '*',        ['OTROS_IMP']),
    # Exento: NO discrimina IVA
    ('COMPRAS', 'EXENTO',      '*',                    '*',        ['OTROS_IMP']),
    ('COMPRAS', 'NO_RESPONSABLE','*',                  '*',        ['OTROS_IMP']),
    ('COMPRAS', 'CONSUMIDOR_FINAL','*',                '*',        ['OTROS_IMP']),

    # ── VENTAS ───────────────────────────────────────────────────────────────
    ('VENTAS',  'RI',          '*',  '*', ['IVA_21','IVA_10_5','IVA_27','OTROS_IMP']),
    ('VENTAS',  'MONOTRIBUTO', '*',  '*', ['OTROS_IMP']),
    ('VENTAS',  'EXENTO',      '*',  '*', ['OTROS_IMP']),
    ('VENTAS',  'CONSUMIDOR_FINAL','*','*',['IVA_21','OTROS_IMP']),  # Factura B

    # ── COBRANZAS ─────────────────────────────────────────────────────────────
    ('COBRANZAS','RI',         'ARBA',                 '*', ['IIBB_ARBA','OTROS_IMP']),
    ('COBRANZAS','RI',         'AGIP',                 '*', ['IIBB_AGIP','OTROS_IMP']),
    ('COBRANZAS','RI',         'AMBOS',                '*', ['IIBB_ARBA','IIBB_AGIP','OTROS_IMP']),
    ('COBRANZAS','RI',         'CONVENIO_MULTILATERAL','*', ['IIBB_CM','OTROS_IMP']),
    ('COBRANZAS','*',          '*',                    '*', ['OTROS_IMP']),

    # ── PAGOS ─────────────────────────────────────────────────────────────────
    ('PAGOS',   'RI',          '*',  '*', ['PERC_IVA','IIBB_ARBA','IIBB_AGIP','OTROS_IMP']),
    ('PAGOS',   'MONOTRIBUTO', '*',  '*', ['OTROS_IMP']),
    ('PAGOS',   '*',           '*',  '*', ['OTROS_IMP']),
]

SEED_REGLAS_IIBB = [
    # (condicion, jur_cod, jur_nom, imp_cod, usa_padron, regimen, limite_cm, coef_min)
    ('ARBA',                  900,  'Buenos Aires',  'IIBB_ARBA', 1, 'LOCAL',      100.00, 0.0000),
    ('AGIP',                  901,  'CABA',          'IIBB_AGIP', 1, 'LOCAL',      100.00, 0.0000),
    ('AMBOS',                 900,  'Buenos Aires',  'IIBB_ARBA', 1, 'LOCAL',      100.00, 0.0000),
    ('AMBOS',                 901,  'CABA',          'IIBB_AGIP', 1, 'LOCAL',      100.00, 0.0000),
    # Convenio Multilateral: sin jurisdicción fija, el usuario la ingresa por factura
    # Límite 50% de la alícuota normal, coeficiente mínimo 0.1000 (reglamentación CM)
    ('CONVENIO_MULTILATERAL', None, 'Multilateral',  'IIBB_CM',   0, 'CM_GENERAL',  50.00, 0.1000),
    ('CM',                    None, 'Multilateral',  'IIBB_CM',   0, 'CM_GENERAL',  50.00, 0.1000),
]


def run():
    print("🚀 Tax Engine — Creando tablas y datos semilla...")
    with get_db_cursor(dictionary=True) as cursor:

        # 1. Crear tablas
        for i, ddl in enumerate(DDL, 1):
            cursor.execute(ddl)
            print(f"  ✅ Tabla {i}/{len(DDL)} creada/verificada.")

        # 2. Seed impuestos
        print(f"\n📦 Insertando {len(SEED_IMPUESTOS)} impuestos...")
        for codigo, nombre, tipo, desc, orden in SEED_IMPUESTOS:
            cursor.execute("""
                INSERT IGNORE INTO tax_impuestos (codigo, nombre, tipo, descripcion, orden_display)
                VALUES (%s, %s, %s, %s, %s)
            """, (codigo, nombre, tipo, desc, orden))

        # Mapa codigo → id
        cursor.execute("SELECT id, codigo FROM tax_impuestos")
        imp_map = {r['codigo']: r['id'] for r in cursor.fetchall()}

        # 3. Seed alícuotas (enterprise_id=0)
        print(f"📦 Insertando alícuotas globales...")
        for codigo, alicuota, base in SEED_ALICUOTAS:
            imp_id = imp_map.get(codigo)
            if not imp_id:
                continue
            cursor.execute("""
                INSERT IGNORE INTO tax_alicuotas
                    (enterprise_id, impuesto_id, alicuota, base_calculo, vigencia_desde)
                VALUES (0, %s, %s, %s, '2000-01-01')
            """, (imp_id, alicuota, base))

        # 4. Seed reglas (enterprise_id=0)
        print(f"📦 Insertando {len(SEED_REGLAS)} reglas fiscales...")
        for operacion, tipo_resp, cond_iibb, exencion, codigos in SEED_REGLAS:
            for codigo in codigos:
                imp_id = imp_map.get(codigo)
                if not imp_id:
                    continue
                cursor.execute("""
                    INSERT IGNORE INTO tax_reglas
                        (enterprise_id, operacion, tipo_responsable, condicion_iibb, exencion_iibb, impuesto_id, aplica)
                    VALUES (0, %s, %s, %s, %s, %s, 1)
                """, (operacion, tipo_resp, cond_iibb, exencion, imp_id))

        # 5. Seed reglas IIBB (enterprise_id=0)
        print(f"📦 Insertando reglas IIBB...")
        for cond, jur_cod, jur_nom, imp_cod, usa_padron, regimen, limite, coef in SEED_REGLAS_IIBB:
            imp_id = imp_map.get(imp_cod)
            if not imp_id:
                continue
            cursor.execute("""
                INSERT IGNORE INTO tax_reglas_iibb
                    (enterprise_id, condicion_iibb, jurisdiccion_codigo, jurisdiccion_nombre,
                     impuesto_id, usa_padron, regimen, limite_cm_pct, coef_minimo_cm)
                VALUES (0, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (cond, jur_cod, jur_nom, imp_id, usa_padron, regimen, limite, coef))

    print("\n✅ Tax Engine: migración completada exitosamente.")
    print("   Tablas: tax_impuestos, tax_alicuotas, tax_reglas, tax_reglas_iibb")
    print("   enterprise_id=0 = plantilla global (heredada por todas las empresas)")


if __name__ == '__main__':
    run()
