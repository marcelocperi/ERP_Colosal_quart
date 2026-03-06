from multiMCP.database import get_db_cursor

def ensure_comprobante_po_link():
    with get_db_cursor() as cursor:
        try:
            print("Verificando columna orden_compra_id en erp_comprobantes...")
            cursor.execute("SHOW COLUMNS FROM erp_comprobantes LIKE 'orden_compra_id'")
            if not cursor.fetchone():
                print("Añadiendo columna orden_compra_id...")
                cursor.execute("ALTER TABLE erp_comprobantes ADD COLUMN orden_compra_id INT AFTER tercero_id")
                # Intento de agregar FK (ignore error if no OC table yet or already exists)
                try:
                    cursor.execute("ALTER TABLE erp_comprobantes ADD CONSTRAINT fk_comp_po FOREIGN KEY (orden_compra_id) REFERENCES cmp_ordenes_compra(id)")
                except: pass
            print("✅ Columna verificada.")
            
            # También necesitamos detalle_po_id en el detalle del comprobante para el 3-way match exacto
            print("Verificando columna detalle_po_id en erp_comprobantes_detalle...")
            cursor.execute("SHOW COLUMNS FROM erp_comprobantes_detalle LIKE 'detalle_po_id'")
            if not cursor.fetchone():
                print("Añadiendo columna detalle_po_id...")
                cursor.execute("ALTER TABLE erp_comprobantes_detalle ADD COLUMN detalle_po_id INT AFTER articulo_id")
            print("✅ Detalle verificado.")
            
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    ensure_comprobante_po_link()
