from database import get_db_cursor

def setup_missing_tables():
    with get_db_cursor() as cursor:
        # Detalle de comprobantes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_comprobantes_detalle (
                id INT AUTO_INCREMENT PRIMARY KEY,
                comprobante_id INT NOT NULL,
                articulo_id INT NOT NULL,
                cantidad DECIMAL(15, 2),
                precio_unitario DECIMAL(15, 2),
                alicuota_iva DECIMAL(5, 2),
                subtotal_neto DECIMAL(15, 2),
                importe_iva DECIMAL(15, 2),
                FOREIGN KEY (comprobante_id) REFERENCES erp_comprobantes(id) ON DELETE CASCADE
            )
        """)
        
        # Tipos de comprobante
        tipos = [
            (0, "001", "Factura A", "A"),
            (0, "006", "Factura B", "B"),
            (0, "011", "Factura C", "C"),
            (0, "003", "Nota de Crédito A", "A"),
            (0, "008", "Nota de Crédito B", "B"),
            (0, "013", "Nota de Crédito C", "C"),
            (0, "002", "Nota de Débito A", "A"),
            (0, "007", "Nota de Débito B", "B"),
            (0, "012", "Nota de Débito C", "C"),
        ]
        for t in tipos:
            cursor.execute("INSERT IGNORE INTO sys_tipos_comprobante (enterprise_id, codigo, descripcion, letra) VALUES (%s, %s, %s, %s)", t)
        
        # Jurisdicciones
        juris = [
            (901, "CABA", "CABA"),
            (902, "Buenos Aires", "BSAS"),
            (903, "Catamarca", "CAT"),
            (904, "Córdoba", "CBA"),
        ]
        for j in juris:
            cursor.execute("INSERT IGNORE INTO sys_jurisdicciones (codigo, nombre, abreviatura) VALUES (%s, %s, %s)", j)
        
        print("Tablas y datos maestros configurados correctamente.")

if __name__ == "__main__":
    setup_missing_tables()
