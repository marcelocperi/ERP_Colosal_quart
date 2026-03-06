
import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def execute_consignment_migration():
    """
    Crea las tablas necesarias para el Módulo de Consignación.
    Cubre: Producción a Fazón, Tenencia para Clientes y Consignación de Proveedores.
    """
    try:
        with get_db_cursor() as cursor:
            print("--- Creando Tablas de Consignación (Fase 1.5) ---")
            
            # 1. Cabecera de Consignación
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cmp_consignaciones (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id INT NOT NULL,
                    tipo ENUM('EXTERNA_SALIDA', 'INTERNA_ENTRADA', 'TENENCIA_CLIENTE') NOT NULL,
                    tercero_id INT NOT NULL,
                    deposito_id INT, -- Depósito virtual donde reside el stock
                    fecha_inicio DATETIME DEFAULT CURRENT_TIMESTAMP,
                    fecha_limite_devolucion DATETIME,
                    estado ENUM('ABIERTA', 'LIQUIDADA', 'DEVUELTA', 'CERRADA_PARCIAL') DEFAULT 'ABIERTA',
                    referencia_doc VARCHAR(50), -- Remito original
                    user_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX(enterprise_id),
                    INDEX(tercero_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 2. Detalles de Consignación
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cmp_items_consignacion (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    consignacion_id INT NOT NULL,
                    articulo_id INT NOT NULL,
                    cantidad_original DECIMAL(18,4) NOT NULL,
                    cantidad_consumida DECIMAL(18,4) DEFAULT 0,
                    cantidad_devuelta DECIMAL(18,4) DEFAULT 0,
                    costo_unitario_pactado DECIMAL(18,4), -- Base para liquidación financiera
                    moneda_id INT DEFAULT 1,
                    user_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (consignacion_id) REFERENCES cmp_consignaciones(id) ON DELETE CASCADE,
                    INDEX(articulo_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 3. Registro de Consumos (Liquidación)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cmp_liquidaciones_consignacion (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    consignacion_item_id INT NOT NULL,
                    cantidad_liquidada DECIMAL(18,4) NOT NULL,
                    tipo_evento ENUM('VENTA', 'USO_PRODUCCION', 'PERDIDA') NOT NULL,
                    comprobante_id INT, -- Factura que liquida el consumo
                    fecha_evento DATETIME DEFAULT CURRENT_TIMESTAMP,
                    user_id INT,
                    FOREIGN KEY (consignacion_item_id) REFERENCES cmp_items_consignacion(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            print("TABLAS DE CONSIGNACION CREADAS CORRECTAMENTE.")

    except Exception as e:
        print(f"Error en migración de consignación: {e}")

if __name__ == "__main__":
    execute_consignment_migration()
