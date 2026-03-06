from database import get_db_cursor

def migrate_logistica():
    print("--- Migración de Entidades Logísticas ---")
    with get_db_cursor() as cursor:
        # 1. Crear tabla de empresas logísticas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_logisticas (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                nombre VARCHAR(100) NOT NULL,
                cuit VARCHAR(11),
                direccion VARCHAR(255),
                calle VARCHAR(100),
                numero VARCHAR(20),
                localidad VARCHAR(100),
                provincia VARCHAR(100),
                email VARCHAR(100),
                telefono VARCHAR(50),
                activo TINYINT(1) DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        print("Tabla 'stk_logisticas' creada/verificada.")

        # 2. Agregar logistica_id a stk_transferencias
        cursor.execute("SHOW COLUMNS FROM stk_transferencias LIKE 'logistica_id'")
        if not cursor.fetchone():
            print("Agregando columna 'logistica_id' a stk_transferencias...")
            cursor.execute("ALTER TABLE stk_transferencias ADD COLUMN logistica_id INT AFTER destino_id")
        
        # 3. Agregar tipo_transporte a stk_transferencias (Propio, Terceros/Logistica)
        cursor.execute("SHOW COLUMNS FROM stk_transferencias LIKE 'tipo_transporte'")
        if not cursor.fetchone():
            print("Agregando columna 'tipo_transporte' a stk_transferencias...")
            cursor.execute("ALTER TABLE stk_transferencias ADD COLUMN tipo_transporte ENUM('PROPIO', 'LOGISTICA') DEFAULT 'PROPIO' AFTER logistica_id")

        # 4. Agregar campos para destino final (cuando va via logistica)
        cursor.execute("SHOW COLUMNS FROM stk_transferencias LIKE 'destino_final_direccion'")
        if not cursor.fetchone():
            print("Agregando campos de destino final a stk_transferencias...")
            cursor.execute("ALTER TABLE stk_transferencias ADD COLUMN destino_final_direccion VARCHAR(255) AFTER destino_id")

    print("Migración completada exitosamente.")

if __name__ == "__main__":
    migrate_logistica()
