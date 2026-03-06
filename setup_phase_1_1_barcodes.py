# -*- coding: utf-8 -*-
from database import get_db_cursor

def setup_multibarcode_tables():
    print("🌐 Iniciando Setup de Trazabilidad Internacional (Fase 1.1 - GTIN/EAN)...")
    
    with get_db_cursor() as cursor:
        # A. Crear la tabla hija para soportar múltiples códigos de barra por SKU (Alias, Cajas Master)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_articulos_codigos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                articulo_id INT NOT NULL,
                codigo VARCHAR(100) NOT NULL, -- El EAN, UPC, ITF-14, o código del proveedor chino.
                tipo_codigo VARCHAR(20) DEFAULT 'GTIN', -- EAN13, UPC, ITF14, PROVEEDOR, INTERNO
                factor_conversion DECIMAL(18, 4) DEFAULT 1.0000, -- Cuántas unidades base rinde este código. Ej: Escanear una Master Box rinde = 100 UoM base.
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (articulo_id) REFERENCES stk_articulos(id) ON DELETE CASCADE,
                UNIQUE(enterprise_id, codigo) -- El código debe ser único en la empresa (un escaneo no puede arrojar dos productos)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

        # B. Actualizar la tabla madre `stk_articulos` para indicar si requieren lote y unidad de medida si no las tiene
        print("Verificando unidad de medida y flags de trazabilidad en stk_articulos...")
        cursor.execute("SHOW COLUMNS FROM stk_articulos LIKE 'unidad_medida'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE stk_articulos ADD COLUMN unidad_medida VARCHAR(20) DEFAULT 'UN'")

        cursor.execute("SHOW COLUMNS FROM stk_articulos LIKE 'requiere_lote'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE stk_articulos ADD COLUMN requiere_lote BOOLEAN DEFAULT 0")

    print("✅ Motor multi-código (GTIN/UoM Conversions) instalado exitosamente.")

if __name__ == "__main__":
    setup_multibarcode_tables()
