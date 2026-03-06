# -*- coding: utf-8 -*-
import datetime
from database import get_db_cursor

def setup_budgeting_tables():
    print("🚀 Iniciando Setup de Tablas de Presupuesto (Fase 2)...")
    
    with get_db_cursor() as cursor:
        # 1. Centros de Costos (Hierarchy supported)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_cost_centers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                code VARCHAR(20) NOT NULL,
                name VARCHAR(100) NOT NULL,
                description VARCHAR(255),
                parent_id INT DEFAULT NULL,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_id) REFERENCES sys_cost_centers(id) ON DELETE SET NULL,
                UNIQUE(enterprise_id, code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 2. Presupuestos (Anuales/Mensuales)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_budgets (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                cost_center_id INT NOT NULL,
                year INT NOT NULL,
                month INT NOT NULL, -- 0 para presupuesto anual global
                amount_allocated DECIMAL(18, 2) NOT NULL DEFAULT 0,
                status VARCHAR(20) DEFAULT 'ACTIVE', -- ACTIVE, CLOSED, EXCEEDED
                created_by INT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (cost_center_id) REFERENCES sys_cost_centers(id),
                UNIQUE(enterprise_id, cost_center_id, year, month)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 3. Ejecución de Presupuesto (Tracking Comprometido vs Real)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_budget_execution (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                budget_id INT NOT NULL,
                transaction_type ENUM('PO', 'NP', 'FA') NOT NULL, -- Purchase Order, Note of Purchase, Bill
                transaction_id INT NOT NULL,
                amount_committed DECIMAL(18, 2) DEFAULT 0, -- Fondos 'reservados' al crear la PO (APPROVED)
                amount_actual DECIMAL(18, 2) DEFAULT 0, -- Fondos 'gastados' al recibir la Factura
                description VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (budget_id) REFERENCES sys_budgets(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

        # 4. Columna centro_costo_id en Ordenes de Compra y Notas de Pedido
        print("Añadiendo columna centro_costo_id a tablas transaccionales...")
        tables_to_update = ['cmp_ordenes_compra', 'cmp_solicitudes_reposicion'] 
        for table in tables_to_update:
            cursor.execute(f"SHOW COLUMNS FROM {table} LIKE 'centro_costo_id'")
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN centro_costo_id INT DEFAULT NULL")
                cursor.execute(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_cc FOREIGN KEY (centro_costo_id) REFERENCES sys_cost_centers(id)")

    print("✅ Tablas de presupuesto creadas correctamente.")

def seed_initial_cost_centers():
    print("🌱 Sembrando Centros de Costos Iniciales (Empresa 0)...")
    now = datetime.datetime.now()
    
    with get_db_cursor(dictionary=True) as cursor:
        # Centros de Costos
        cc_data = [
            ('ADM', 'Administración y Finanzas', 'Gerencia Administrativa'),
            ('MKT', 'Marketing y Ventas', 'Publicidad y Comercialización'),
            ('OPS', 'Operaciones y Logística', 'Almacén y Distribución'),
            ('IT', 'Tecnología e Infraestructura', 'Sistemas y Desarrollo'),
            ('RRHH', 'Recursos Humanos', 'Capacitación y Personal')
        ]
        
        for code, name, desc in cc_data:
            cursor.execute("SELECT id FROM sys_cost_centers WHERE enterprise_id = 0 AND code = %s", (code,))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO sys_cost_centers (enterprise_id, code, name, description)
                    VALUES (0, %s, %s, %s)
                """, (code, name, desc))
                cc_id = cursor.lastrowid
                
                # Crear presupuesto muestra para el mes actual ($10,000 por defecto)
                cursor.execute("""
                    INSERT INTO sys_budgets (enterprise_id, cost_center_id, year, month, amount_allocated)
                    VALUES (0, %s, %s, %s, %s)
                """, (cc_id, now.year, now.month, 10000.00))

    print("✅ Centros de costos y presupuestos base sembrados.")

if __name__ == "__main__":
    setup_budgeting_tables()
    seed_initial_cost_centers()
