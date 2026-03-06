-- ==============================================================================
-- STOCK & INVENTORY MODULE (PROFESSIONAL)
-- ==============================================================================
-- 1. ALMACENES / DEPOSITOS
CREATE TABLE IF NOT EXISTS stk_depositos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    direccion VARCHAR(200),
    es_principal BOOLEAN DEFAULT 0,
    activo BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 2. MOTIVOS DE MOVIMIENTO (Reason Codes)
CREATE TABLE IF NOT EXISTS stk_motivos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    tipo VARCHAR(20) NOT NULL,
    -- 'ENTRADA', 'SALIDA', 'TRANSFERENCIA', 'AJUSTE'
    automatico BOOLEAN DEFAULT 0,
    -- 1=System generated, 0=Manual
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 3. MOVIMIENTOS CABECERA (Transactions Header)
CREATE TABLE IF NOT EXISTS stk_movimientos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
    motivo_id INT NOT NULL,
    deposito_origen_id INT,
    deposito_destino_id INT,
    comprobante_id INT,
    -- Link to Invoice/Credit Note
    tercero_id INT,
    -- Link to erp_terceros
    user_id INT,
    observaciones TEXT,
    estado VARCHAR(20) DEFAULT 'CONFIRMADO',
    FOREIGN KEY (motivo_id) REFERENCES stk_motivos(id),
    FOREIGN KEY (deposito_origen_id) REFERENCES stk_depositos(id),
    FOREIGN KEY (deposito_destino_id) REFERENCES stk_depositos(id),
    FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id)
);
-- 4. MOVIMIENTOS DETALLE (Transactions Items)
CREATE TABLE IF NOT EXISTS stk_movimientos_detalle (
    id INT AUTO_INCREMENT PRIMARY KEY,
    movimiento_id INT NOT NULL,
    articulo_id INT NOT NULL,
    cantidad INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (movimiento_id) REFERENCES stk_movimientos(id) ON DELETE CASCADE
);
-- 5. EXISTENCIAS ACTUALES (Current Stock Snapshot)
CREATE TABLE IF NOT EXISTS stk_existencias (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    deposito_id INT NOT NULL,
    articulo_id INT NOT NULL,
    cantidad INT DEFAULT 0,
    ubicacion VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE(enterprise_id, deposito_id, articulo_id),
    FOREIGN KEY (deposito_id) REFERENCES stk_depositos(id)
);
-- 6. TIPOS DE ARTICULOS
CREATE TABLE IF NOT EXISTS stk_tipos_articulo (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    naturaleza VARCHAR(50) DEFAULT 'GENERAL',
    cuenta_contable_compra_id INT,
    cuenta_contable_venta_id INT,
    activo BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- SEED DATA (Template Global - Enterprise 0)
INSERT IGNORE INTO stk_depositos (enterprise_id, nombre, es_principal)
VALUES (0, 'Depósito Central', 1);
INSERT IGNORE INTO stk_motivos (enterprise_id, nombre, tipo, automatico)
VALUES (0, 'Venta (Facturación)', 'SALIDA', 1),
    (0, 'Compra (Recepción)', 'ENTRADA', 1),
    (0, 'Devolución de Cliente', 'ENTRADA', 1),
    (0, 'Ajuste de Inventario (+)', 'ENTRADA', 0),
    (0, 'Ajuste de Inventario (-)', 'SALIDA', 0),
    (
        0,
        'Transferencia entre Depósitos',
        'TRANSFERENCIA',
        0
    );
INSERT IGNORE INTO stk_tipos_articulo (enterprise_id, nombre, naturaleza)
VALUES (0, 'Libro', 'EDITORIAL'),
    (0, 'Repuesto', 'MECANICA'),
    (0, 'Insumo Oficina', 'INSUMOS'),
    (0, 'Servicio', 'SERVICIOS');