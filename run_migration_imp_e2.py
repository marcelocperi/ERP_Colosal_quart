#!/usr/bin/env python3
"""
Migración Etapa 2 — Módulo de Importaciones
  - imp_despachos: Tabla de despachos aduaneros
  - imp_cargos ajuste: columna estado
  - stk_articulos: columna costo_importacion_ultimo
Ejecutar: python run_migration_imp_e2.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from database import get_db_cursor
    print("[OK] database importada")
except ImportError as e:
    print(f"[ERROR] {e}"); sys.exit(1)

SQL_STEPS = [

    # ── 1. DESPACHOS ADUANEROS ─────────────────────────────────────────────
    ("imp_despachos: CREATE", """
        CREATE TABLE IF NOT EXISTS imp_despachos (
            id                  INT AUTO_INCREMENT PRIMARY KEY,
            enterprise_id       INT NOT NULL,
            orden_compra_id     INT NOT NULL,
            numero_despacho     VARCHAR(40) DEFAULT NULL,
            despachante_id      INT DEFAULT NULL,
            fecha_oficializacion DATE DEFAULT NULL,
            fecha_liberacion    DATE DEFAULT NULL,
            canal               ENUM('VERDE','AMARILLO','ROJO') DEFAULT 'VERDE',
            estado              ENUM('PENDIENTE','PRESENTADO','EN_REVISION','OBSERVADO','LIBERADO','INGRESADO')
                                DEFAULT 'PENDIENTE',
            valor_fob_usd       DECIMAL(18,2) DEFAULT 0,
            valor_cif_usd       DECIMAL(18,2) DEFAULT 0,
            derechos_ars        DECIMAL(18,2) DEFAULT 0,
            tasa_estadistica_ars DECIMAL(18,2) DEFAULT 0,
            otros_tributos_ars  DECIMAL(18,2) DEFAULT 0,
            tipo_cambio_oficializacion DECIMAL(18,6) DEFAULT NULL,
            observaciones       TEXT DEFAULT NULL,
            user_id             INT DEFAULT NULL,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """),

    # ── 2. ÍTEMS POR DESPACHO (vincula despacho ↔ items de la OC y sus costos) ──
    ("imp_despachos_items: CREATE", """
        CREATE TABLE IF NOT EXISTS imp_despachos_items (
            id                  INT AUTO_INCREMENT PRIMARY KEY,
            despacho_id         INT NOT NULL,
            orden_compra_id     INT NOT NULL,
            articulo_id         INT NOT NULL,
            cantidad            DECIMAL(14,4) NOT NULL DEFAULT 0,
            precio_unitario_usd DECIMAL(18,4) DEFAULT 0,
            valor_total_usd     DECIMAL(18,2) DEFAULT 0,
            -- Costos distribuidos desde imp_cargos
            costo_flete_usd     DECIMAL(18,4) DEFAULT 0,
            costo_seguro_usd    DECIMAL(18,4) DEFAULT 0,
            costo_derechos_ars  DECIMAL(18,4) DEFAULT 0,
            costo_otros_ars     DECIMAL(18,4) DEFAULT 0,
            -- CUI final en ARS (Costo Unitario de Importacion)
            cui_ars             DECIMAL(18,4) DEFAULT 0,
            FOREIGN KEY (despacho_id) REFERENCES imp_despachos(id) ON DELETE CASCADE
        )
    """),

    # ── 3. COLUMNAS ADICIONALES EN imp_cargos ─────────────────────────────
    ("imp_cargos: estado",
     "ALTER TABLE imp_cargos ADD COLUMN IF NOT EXISTS estado VARCHAR(20) DEFAULT 'PENDIENTE'"),
    ("imp_cargos: despacho_id",
     "ALTER TABLE imp_cargos ADD COLUMN IF NOT EXISTS despacho_id INT DEFAULT NULL"),
    ("imp_cargos: aplica_a_cui",
     "ALTER TABLE imp_cargos ADD COLUMN IF NOT EXISTS aplica_a_cui TINYINT(1) DEFAULT 1 COMMENT '1=incluir en CUI, 0=excluir'"),

    # ── 4. COLUMNA EN stk_articulos (snapshot del último CUI registrado) ───
    ("stk_articulos: costo_importacion_ultimo",
     "ALTER TABLE stk_articulos ADD COLUMN IF NOT EXISTS costo_importacion_ultimo DECIMAL(18,4) DEFAULT NULL"),
    ("stk_articulos: fecha_costo_importacion",
     "ALTER TABLE stk_articulos ADD COLUMN IF NOT EXISTS fecha_costo_importacion DATE DEFAULT NULL"),

    # ── 5. ESTADO EN imp_documentos ──────────────────────────────────────
    ("imp_documentos: despacho_id FK",
     "ALTER TABLE imp_documentos ADD COLUMN IF NOT EXISTS despacho_id INT DEFAULT NULL"),

    # ── 6. COLUMNA EN cmp_ordenes_compra: estado_importacion ─────────────
    ("cmp_ordenes_compra: estado_importacion",
     "ALTER TABLE cmp_ordenes_compra ADD COLUMN IF NOT EXISTS estado_importacion VARCHAR(30) DEFAULT NULL COMMENT 'DOCUMENTACION → DESPACHO → LIBERADO → INGRESADO'"),

    # ── 7. ÍNDICES ────────────────────────────────────────────────────────
    ("idx imp_despachos orden",
     "CREATE INDEX IF NOT EXISTS idx_despachos_orden ON imp_despachos (orden_compra_id)"),
    ("idx imp_despachos_items despacho",
     "CREATE INDEX IF NOT EXISTS idx_despachos_items ON imp_despachos_items (despacho_id)"),
    ("idx imp_despachos_items articulo",
     "CREATE INDEX IF NOT EXISTS idx_despachos_art ON imp_despachos_items (articulo_id)"),
]


def run():
    ok = warn = err = 0
    print("\n" + "="*60)
    print("  MIGRACIÓN ETAPA 2 — Importaciones: Costos + Aduana")
    print("="*60)
    with get_db_cursor() as cursor:
        for label, sql in SQL_STEPS:
            try:
                cursor.execute(sql.strip())
                print(f"  [✓] {label}")
                ok += 1
            except Exception as e:
                msg = str(e)
                if any(k in msg for k in ["Duplicate column", "already exists", "Can't DROP", "Duplicate key"]):
                    print(f"  [~] {label} — ya existía (OK)")
                    warn += 1
                else:
                    print(f"  [✗] {label}")
                    print(f"       → {msg}")
                    err += 1
    print("\n" + "-"*60)
    print(f"  Resultado: {ok} OK  |  {warn} omitidos  |  {err} errores")
    print("="*60 + "\n")
    return err == 0

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
