from database import get_db_cursor

def migrate_stock_advanced():
    try:
        with get_db_cursor() as cursor:
            # 1. Update stk_depositos
            print("--- Actualizando stk_depositos ---")
            cursor.execute("SHOW COLUMNS FROM stk_depositos LIKE 'tipo'")
            if not cursor.fetchone():
                print("Agregando columna 'tipo'...")
                cursor.execute("ALTER TABLE stk_depositos ADD COLUMN tipo ENUM('INTERNO', 'SATELITE', 'CONSIGNACION_PROPIA', 'CONSIGNACION_TERCERO') DEFAULT 'INTERNO' AFTER nombre")
            
            cursor.execute("SHOW COLUMNS FROM stk_depositos LIKE 'tercero_id'")
            if not cursor.fetchone():
                print("Agregando columna 'tercero_id' (para depósitos satélites/consignados)...")
                cursor.execute("ALTER TABLE stk_depositos ADD COLUMN tercero_id INT AFTER tipo")

            # 2. Add stk_inventarios (Auditoria/Control)
            print("--- Creando tablas de control de inventario ---")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stk_inventarios (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id INT NOT NULL,
                    deposito_id INT NOT NULL,
                    fecha_inicio DATETIME DEFAULT CURRENT_TIMESTAMP,
                    fecha_cierre DATETIME NULL,
                    tipo ENUM('CICLICO', 'DIRIGIDO', 'GENERAL') DEFAULT 'GENERAL',
                    estado ENUM('BORRADOR', 'EN_PROCESO', 'CERRADO', 'CANCELADO') DEFAULT 'BORRADOR',
                    responsable_id INT,
                    observaciones TEXT,
                    criteria_json TEXT, -- Para inventario dirigido (ej: solo una marca, o una categoria)
                    FOREIGN KEY (deposito_id) REFERENCES stk_depositos(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stk_items_inventario (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    inventario_id INT NOT NULL,
                    articulo_id INT NOT NULL,
                    stock_sistema DECIMAL(12,4) DEFAULT 0,
                    stock_fisico DECIMAL(12,4) DEFAULT 0,
                    diferencia DECIMAL(12,4) GENERATED ALWAYS AS (stock_fisico - stock_sistema) STORED,
                    ajustado TINYINT(1) DEFAULT 0,
                    FOREIGN KEY (inventario_id) REFERENCES stk_inventarios(id),
                    FOREIGN KEY (articulo_id) REFERENCES stk_articulos(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 3. Add stk_transferencias (Header for multi-item transfers)
            print("--- Creando tabla de transferencias entre depósitos ---")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stk_transferencias (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id INT NOT NULL,
                    origen_id INT NOT NULL,
                    destino_id INT NOT NULL,
                    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
                    estado ENUM('PENDIENTE', 'EN_TRANSITO', 'RECIBIDO', 'CANCELADO') DEFAULT 'PENDIENTE',
                    motivo VARCHAR(100),
                    usuario_id INT,
                    FOREIGN KEY (origen_id) REFERENCES stk_depositos(id),
                    FOREIGN KEY (destino_id) REFERENCES stk_depositos(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stk_items_transferencia (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    transferencia_id INT NOT NULL,
                    articulo_id INT NOT NULL,
                    cantidad DECIMAL(12,4) NOT NULL,
                    FOREIGN KEY (transferencia_id) REFERENCES stk_transferencias(id),
                    FOREIGN KEY (articulo_id) REFERENCES stk_articulos(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            print("\n✅ Migración de Stock Avanzado completada.")

    except Exception as e:
        print(f"Error en migración: {e}")

if __name__ == "__main__":
    migrate_stock_advanced()
