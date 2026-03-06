
import sys
import os
# Agregar el directorio multiMCP al path para importar database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_cursor

def migrate():
    with get_db_cursor() as cursor:
        print("Creating sys_invoice_layouts table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_invoice_layouts (
                id INTEGER NOT NULL AUTO_INCREMENT,
                enterprise_id INTEGER NOT NULL,
                field_name VARCHAR(100) NOT NULL,
                x FLOAT NOT NULL,
                y FLOAT NOT NULL,
                font_size FLOAT DEFAULT 10,
                font_style VARCHAR(20) DEFAULT 'normal',
                alignment VARCHAR(10) DEFAULT 'left',
                section VARCHAR(50) DEFAULT 'general',
                PRIMARY KEY (id),
                UNIQUE KEY (enterprise_id, field_name)
            ) COLLATE utf8mb4_unicode_ci DEFAULT CHARSET=utf8mb4 ENGINE=InnoDB;
        """)

        # Seed data for enterprise_id = 0 (Global Default)
        # Based on invoice_analysis.txt coordinates
        layout_data = [
            # HEADER - BLOQUE IZQUIERDO (EMISOR)
            (0, 'emisor_nombre', 22.0, 45.0, 13.0, 'bold', 'left', 'header'),
            (0, 'label_emisor_rsocial', 22.0, 85.0, 9.0, 'bold', 'left', 'header'),
            (0, 'emisor_razon_social', 105.0, 85.0, 9.0, 'normal', 'left', 'header'),
            (0, 'label_emisor_domicilio', 22.0, 105.0, 9.0, 'bold', 'left', 'header'),
            (0, 'emisor_domicilio', 110.0, 105.0, 9.0, 'normal', 'left', 'header'),
            (0, 'label_emisor_condicion', 22.0, 135.0, 9.0, 'bold', 'left', 'header'),
            (0, 'emisor_condicion_iva', 145.0, 135.0, 9.0, 'normal', 'left', 'header'),

            # HEADER - BLOQUE CENTRAL (LETRA)
            (0, 'ejemplar_tag', 298.5, 5.0, 11.0, 'bold', 'center', 'header'),
            (0, 'letra', 298.5, 30.0, 32.0, 'bold', 'center', 'header'),
            (0, 'tipo_comprobante_codigo', 298.5, 60.0, 8.0, 'bold', 'center', 'header'),

            # HEADER - BLOQUE DERECHO (COMPROBANTE)
            (0, 'tipo_comprobante_nombre', 440.0, 45.0, 18.0, 'bold', 'center', 'header'),
            (0, 'label_punto_venta', 345.0, 65.0, 9.0, 'bold', 'left', 'header'),
            (0, 'punto_venta', 435.0, 65.0, 10.0, 'bold', 'left', 'header'),
            (0, 'label_numero', 485.0, 65.0, 9.0, 'bold', 'left', 'header'),
            (0, 'numero', 540.0, 65.0, 10.0, 'bold', 'left', 'header'),
            (0, 'label_fecha_emision', 345.0, 80.0, 9.0, 'bold', 'left', 'header'),
            (0, 'fecha_emision', 445.0, 80.0, 10.0, 'normal', 'left', 'header'),
            (0, 'label_emisor_cuit', 345.0, 105.0, 9.0, 'bold', 'left', 'header'),
            (0, 'emisor_cuit', 380.0, 105.0, 9.0, 'normal', 'left', 'header'),
            (0, 'label_emisor_iibb', 345.0, 120.0, 9.0, 'bold', 'left', 'header'),
            (0, 'emisor_iibb', 435.0, 120.0, 9.0, 'normal', 'left', 'header'),
            (0, 'label_emisor_inicio', 345.0, 135.0, 9.0, 'bold', 'left', 'header'),
            (0, 'emisor_inicio_actividades', 505.0, 135.0, 9.0, 'normal', 'left', 'header'),

            # PERIODO
            (0, 'label_periodo_desde', 22.0, 177.0, 9.0, 'bold', 'left', 'period'),
            (0, 'periodo_desde', 140.0, 177.0, 9.0, 'normal', 'left', 'period'),
            (0, 'label_periodo_hasta', 230.0, 177.0, 9.0, 'bold', 'left', 'period'),
            (0, 'periodo_hasta', 265.0, 177.0, 9.0, 'normal', 'left', 'period'),
            (0, 'label_vencimiento_pago', 360.0, 177.0, 9.0, 'bold', 'left', 'period'),
            (0, 'vencimiento_pago', 500.0, 177.0, 9.0, 'normal', 'left', 'period'),

            # CLIENTE
            (0, 'label_cliente_cuit', 22.0, 201.0, 9.0, 'bold', 'left', 'client'),
            (0, 'cliente_cuit', 50.0, 201.0, 9.0, 'normal', 'left', 'client'),
            (0, 'label_cliente_rsocial', 220.0, 201.0, 9.0, 'bold', 'left', 'client'),
            (0, 'cliente_nombre', 385.0, 201.0, 9.0, 'normal', 'left', 'client'),
            (0, 'label_cliente_iva', 22.0, 218.0, 9.0, 'bold', 'left', 'client'),
            (0, 'cliente_condicion_iva', 130.0, 218.0, 9.0, 'normal', 'left', 'client'),
            (0, 'label_cliente_domicilio', 310.0, 218.0, 9.0, 'bold', 'left', 'client'),
            (0, 'cliente_domicilio', 360.0, 218.0, 9.0, 'normal', 'left', 'client'),
            (0, 'label_condicion_venta', 22.0, 235.0, 9.0, 'bold', 'left', 'client'),
            (0, 'condicion_venta', 110.0, 235.0, 9.0, 'normal', 'left', 'client'),

            # REFERENCIA COMERCIAL
            (0, 'label_referencia', 22.0, 264.0, 9.0, 'bold', 'left', 'client'),
            (0, 'referencia_comercial', 120.0, 264.0, 9.0, 'normal', 'left', 'client'),

            # FOOTER / LEGAL
            (0, 'qr_code', 30.0, 720.0, 0, 'normal', 'left', 'footer'),
            (0, 'label_cae', 440.0, 745.0, 9.0, 'bold', 'left', 'footer'),
            (0, 'cae_value', 520.0, 745.0, 9.0, 'normal', 'left', 'footer'),
            (0, 'label_vto_cae', 370.0, 765.0, 9.0, 'bold', 'left', 'footer'),
            (0, 'vto_cae_value', 520.0, 765.0, 9.0, 'normal', 'left', 'footer'),
            (0, 'label_page_info', 298.5, 765.0, 9.0, 'normal', 'center', 'footer'),
            (0, 'barcode', 298.5, 800.0, 0, 'normal', 'center', 'footer'),
            (0, 'label_autorizado', 180.0, 730.0, 10.0, 'bold', 'left', 'footer'),

            # TOTALS
            (0, 'total_subtotal_value', 560.0, 620.0, 10.0, 'bold', 'right', 'totals'),
            (0, 'total_otros_value', 560.0, 640.0, 10.0, 'bold', 'right', 'totals'),
            (0, 'total_total_value', 560.0, 660.0, 11.0, 'bold', 'right', 'totals'),
            (0, 'label_total_subtotal', 440.0, 620.0, 10.0, 'bold', 'left', 'totals'),
            (0, 'label_total_otros', 400.0, 640.0, 10.0, 'bold', 'left', 'totals'),
            (0, 'label_total_total', 430.0, 660.0, 11.0, 'bold', 'left', 'totals'),
            # Faltantes: Tablas de Impuestos (Coordenadas de inicio)
            (0, 'tabla_iva_y', 0, 510.0, 0, 'normal', 'left', 'totals'),
            (0, 'tabla_tributos_y', 0, 510.0, 0, 'normal', 'left', 'totals'),
            (0, 'label_otros_tributos', 20.0, 512.0, 7.0, 'bold', 'left', 'totals'),
        ]

        print("Seeding layout data...")
        for item in layout_data:
            cursor.execute("""
                INSERT INTO sys_invoice_layouts (enterprise_id, field_name, x, y, font_size, font_style, alignment, section)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE x=VALUES(x), y=VALUES(y), font_size=VALUES(font_size), font_style=VALUES(font_style), alignment=VALUES(alignment), section=VALUES(section)
            """, item)
        
        print("✅ Migration complete.")

if __name__ == "__main__":
    migrate()
