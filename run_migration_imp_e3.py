import database
with database.get_db_cursor() as cur:
    print("--- Phase 3 Migration (Finance & Payments) ---")
    
    # 1. Table for International Payments (Wire transfers / SWIFT)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS imp_pagos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            enterprise_id INT NOT NULL,
            orden_compra_id INT,
            proveedor_id INT NOT NULL,
            fecha DATE NOT NULL,
            moneda VARCHAR(10) NOT NULL,
            monto_orig DECIMAL(15,2) NOT NULL,
            tipo_cambio DECIMAL(15,4) NOT NULL,
            monto_ars DECIMAL(15,2) NOT NULL,
            gastos_bancarios_ars DECIMAL(15,2) DEFAULT 0,
            gastos_bancarios_usd DECIMAL(15,2) DEFAULT 0,
            banco_id INT,
            referencia_swift VARCHAR(100),
            estado VARCHAR(20) DEFAULT 'PENDIENTE',
            asiento_id INT,
            observaciones TEXT,
            user_id INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    print("Table imp_pagos created/verified.")

    # 2. Add columns to imp_cargos to link with payments if needed
    try:
        cur.execute("ALTER TABLE imp_cargos ADD COLUMN pago_id INT AFTER despacho_id")
        print("Column pago_id added to imp_cargos.")
    except:
        print("Column pago_id already exists or error adding it.")

    # 3. View/Table for foreign currency balances (Snapshot)
    # We might use erp_terceros columns or a specific table. 
    # Let's add a column to mark if a payment is finalized.
    
    print("Migration finished successfully.")
