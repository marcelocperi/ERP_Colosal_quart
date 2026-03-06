
import sys
import os

# Ensure the root of the project is in path
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from database import get_db_cursor
    import datetime

    def run_migration():
        try:
            with get_db_cursor() as cursor:
                print("--- MSAC Phase 1.1: Sourcing Hardening ---")
                
                # 1. Create cmp_sourcing_origenes
                print("Creando tabla cmp_sourcing_origenes...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cmp_sourcing_origenes (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        enterprise_id INT NOT NULL,
                        nombre VARCHAR(100) NOT NULL,
                        descripcion TEXT,
                        impuestos_arancel_pct DECIMAL(19,4) DEFAULT 0.00,
                        flete_estimado_pct DECIMAL(19,4) DEFAULT 0.00,
                        seguro_estimado_pct DECIMAL(19,4) DEFAULT 0.00,
                        activo TINYINT DEFAULT 1,
                        user_id INT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        user_id_update INT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_ent (enterprise_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                
                # 2. Check and Seed Initial Origins for Enterprise 0 (Management) and maybe others if needed
                cursor.execute("SELECT COUNT(*) FROM cmp_sourcing_origenes WHERE enterprise_id = 0")
                if cursor.fetchone()[0] == 0:
                    print("Insertando orígenes por defecto para Enterprise 0...")
                    cursor.execute("""
                        INSERT INTO cmp_sourcing_origenes (enterprise_id, nombre, descripcion, impuestos_arancel_pct, flete_estimado_pct, user_id)
                        VALUES 
                        (0, 'Local', 'Proveedores del mercado interno (IVA 21%)', 0.0000, 0.0000, 1),
                        (0, 'Importado (China)', 'Importaciones via Freight Forwarder', 15.0000, 8.0000, 1),
                        (0, 'Importado (Europa)', 'Importaciones via Aereo/Courier', 12.0000, 20.0000, 1);
                    """)

                # 3. Create/Update cmp_articulos_proveedores to include origen_id
                print("Asegurando tabla cmp_articulos_proveedores...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cmp_articulos_proveedores (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        enterprise_id INT NOT NULL,
                        articulo_id INT NOT NULL,
                        proveedor_id INT NOT NULL,
                        origen_id INT,
                        codigo_articulo_proveedor VARCHAR(50),
                        precio_referencia DECIMAL(19, 4) DEFAULT 0.00,
                        moneda VARCHAR(10) DEFAULT 'ARS',
                        lead_time_dias INT DEFAULT 0,
                        es_habitual TINYINT DEFAULT 0,
                        user_id INT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        user_id_update INT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        CONSTRAINT unique_art_prov UNIQUE (enterprise_id, articulo_id, proveedor_id),
                        INDEX idx_art (articulo_id),
                        INDEX idx_prov (proveedor_id),
                        INDEX idx_origen (origen_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """)
                
                # Check if origen_id exists in cmp_articulos_proveedores (for when it was created partially before)
                cursor.execute("SHOW COLUMNS FROM cmp_articulos_proveedores LIKE 'origen_id'")
                if not cursor.fetchone():
                    print("Agregando columna origen_id a cmp_articulos_proveedores...")
                    cursor.execute("ALTER TABLE cmp_articulos_proveedores ADD COLUMN origen_id INT AFTER proveedor_id")

                print("MSAC Phase 1.1 COMPLETADA con éxito.")

        except Exception as e:
            print(f"Error durante la migración: {e}")
            raise e

    if __name__ == "__main__":
        run_migration()

except ImportError as e:
    print(f"No se pudo importar el módulo database: {e}")
except Exception as e:
    print(f"Error fatal: {e}")
