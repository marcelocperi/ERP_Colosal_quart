
import sys
import os
# Adjust path to import database module
sys.path.append(os.path.join(os.path.dirname(__file__)))
from database import get_db_cursor

def create_table():
    try:
        with get_db_cursor() as cursor:
            print("Creando tabla cmp_articulos_proveedores...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cmp_articulos_proveedores (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id INT NOT NULL,
                    articulo_id INT NOT NULL,
                    proveedor_id INT NOT NULL,
                    codigo_articulo_proveedor VARCHAR(50),
                    precio_referencia DECIMAL(15, 2) DEFAULT 0.00,
                    moneda VARCHAR(10) DEFAULT 'ARS',
                    lead_time_dias INT DEFAULT 0,
                    es_habitual TINYINT DEFAULT 0,
                    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    CONSTRAINT unique_art_prov UNIQUE (enterprise_id, articulo_id, proveedor_id),
                    INDEX idx_art (articulo_id),
                    INDEX idx_prov (proveedor_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            print("Tabla creada exitosamente.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_table()
