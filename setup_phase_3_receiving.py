# -*- coding: utf-8 -*-
import datetime
from database import get_db_cursor

def setup_receiving_tables():
    print("📦 Iniciando Setup de Tablas de Recepción (Fase 3)...")
    
    with get_db_cursor() as cursor:
        # 1. Cabecera de Recepciones en Depósito (Remito)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_recepciones (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                orden_compra_id INT NOT NULL,
                numero_remito_proveedor VARCHAR(50),
                fecha_recepcion DATE NOT NULL,
                recibido_por INT NOT NULL, -- ID del usuario del depósito
                observaciones TEXT,
                estado VARCHAR(20) DEFAULT 'INGRESADO', -- INGRESADO, CONCILIADO, DEVUELTO
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (orden_compra_id) REFERENCES cmp_ordenes_compra(id),
                FOREIGN KEY (recibido_por) REFERENCES sys_users(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 2. Detalles de lo contado "A Ciegas"
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_detalles_recepcion (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                recepcion_id INT NOT NULL,
                detalle_orden_id INT NOT NULL, -- Referencia a la línea original de la PO
                articulo_id INT NOT NULL,
                cantidad_recibida DECIMAL(18, 4) NOT NULL DEFAULT 0,
                lote VARCHAR(50) DEFAULT NULL,
                vencimiento DATE DEFAULT NULL,
                diferencia_detectada BOOLEAN DEFAULT 0, -- Flag CISA: ¿Recibió distinto a lo pedido?
                FOREIGN KEY (recepcion_id) REFERENCES stk_recepciones(id),
                FOREIGN KEY (detalle_orden_id) REFERENCES cmp_detalles_orden(id),
                FOREIGN KEY (articulo_id) REFERENCES stk_articulos(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

        # 3. Trackear saldos pendientes en la orden original
        print("Añadiendo columna cantidad_recibida a cmp_detalles_orden...")
        cursor.execute("SHOW COLUMNS FROM cmp_detalles_orden LIKE 'cantidad_recibida'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE cmp_detalles_orden ADD COLUMN cantidad_recibida DECIMAL(18, 4) DEFAULT 0")

    print("✅ Tablas de Recepción Ciega creadas correctamente.")

if __name__ == "__main__":
    setup_receiving_tables()
