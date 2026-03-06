
import sys
import os
sys.path.insert(0, os.getcwd())
from database import get_db_cursor

def run_migration():
    with get_db_cursor() as cursor:
        print("--- Iniciando migración de normalización fiscal y Remitos/COT ---")

        # 1. Crear tabla de reglas de tipos de comprobante
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_fiscal_comprobante_rules (
                id INT AUTO_INCREMENT PRIMARY KEY,
                emisor_condicion VARCHAR(100) NOT NULL,
                receptor_condicion VARCHAR(100) NOT NULL,
                allowed_codigos TEXT NOT NULL,
                INDEX (emisor_condicion, receptor_condicion)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        print("   [OK] Tabla sys_fiscal_comprobante_rules verificada/creada.")

        # 2. Poblar reglas (Limpiar primero para evitar duplicados en re-ejecución)
        cursor.execute("DELETE FROM sys_fiscal_comprobante_rules")
        rules = [
            ('MONOTRIBUTISTA', '*', '011,012,013'),
            ('MONOTRIBUTO', '*', '011,012,013'),
            ('RESPONSABLE_INSCRIPTO', 'RESPONSABLE_INSCRIPTO', '001,002,003'),
            ('RESPONSABLE_INSCRIPTO', '*', '006,007,008'),
            ('*', '*', '006,007,008') # Fallback
        ]
        cursor.executemany("""
            INSERT INTO sys_fiscal_comprobante_rules (emisor_condicion, receptor_condicion, allowed_codigos)
            VALUES (%s, %s, %s)
        """, rules)
        print("   [OK] Reglas fiscales normalizadas insertadas.")

        # 3. Asegurar que existe el tipo Remito
        cursor.execute("SELECT codigo FROM sys_tipos_comprobante WHERE codigo = '091'")
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO sys_tipos_comprobante (enterprise_id, codigo, descripcion, letra)
                VALUES (0, '091', 'Remito R', 'R')
            """)
            print("   [OK] Tipo de comprobante '091 - Remito R' creado.")
        else:
            print("   [SKIP] Tipo de comprobante '091' ya existe.")

        # 4. Agregar columna COT a erp_comprobantes
        cursor.execute("SHOW COLUMNS FROM erp_comprobantes LIKE 'cot'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE erp_comprobantes ADD COLUMN cot VARCHAR(50) DEFAULT NULL AFTER numero")
            print("   [OK] Columna 'cot' agregada a erp_comprobantes.")
        else:
            print("   [SKIP] Columna 'cot' ya existe.")

        # 6. Campos de Transporte en erp_comprobantes
        columns_to_add = [
            ('transportista_nombre', 'VARCHAR(200)'),
            ('transportista_cuit', 'VARCHAR(20)'),
            ('vehiculo_patente', 'VARCHAR(20)')
        ]
        for col_name, col_type in columns_to_add:
            cursor.execute(f"SHOW COLUMNS FROM erp_comprobantes LIKE '{col_name}'")
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE erp_comprobantes ADD COLUMN {col_name} {col_type} DEFAULT NULL")
                print(f"   [OK] Columna '{col_name}' agregada.")

        # 7. Campos de Layout para Remito (enterprise_id = 0)
        # label_entrega_domicilio (Lugar de Entrega)
        # entrega_domicilio (Valor)
        # label_disclaimer (Leyenda "No válido como factura")
        layout_fields = [
            ('label_entrega_domicilio', 30, 246, 'header', 'left', 7, 'bold'),
            ('entrega_domicilio', 110, 246, 'header', 'left', 7.5, 'normal'),
            ('label_disclaimer', 298.5, 155, 'header', 'center', 9, 'bold')
        ]
        for f in layout_fields:
            cursor.execute("SELECT id FROM sys_invoice_layouts WHERE field_name = %s AND enterprise_id = 0", (f[0],))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO sys_invoice_layouts 
                    (enterprise_id, field_name, x, y, section, alignment, font_size, font_style)
                    VALUES (0, %s, %s, %s, %s, %s, %s, %s)
                """, f)
                print(f"   [OK] Campo de layout '{f[0]}' creado.")

        print("--- Migración finalizada con éxito ---")

if __name__ == "__main__":
    run_migration()
