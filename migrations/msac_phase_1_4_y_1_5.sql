-- ============================================================
-- MSAC Fase 1.4: RFQ Enrichment (Cotizaciones por Explosión)
-- ============================================================
-- Permitirá que al decir "quiero fabricar 1000 autos", el sistema
-- genere una campaña pidiendo precios de las 4000 ruedas y motores.
CREATE TABLE IF NOT EXISTS cmp_rfq_campanas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    fecha_emision DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_cierre DATETIME NOT NULL,
    estado ENUM('BORRADOR', 'ENVIADA', 'CERRADA', 'ADJUDICADA') DEFAULT 'BORRADOR',
    articulo_objetivo_id INT NULL COMMENT 'El producto a producir que detonó este RFQ',
    cantidad_objetivo DECIMAL(15, 4) NULL COMMENT 'Cantidad a producir',
    user_id INT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_rfq_ent (enterprise_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS cmp_rfq_detalles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rfq_id INT NOT NULL,
    articulo_insumo_id INT NOT NULL,
    cantidad_requerida DECIMAL(15, 4) NOT NULL,
    sugerencia_origen VARCHAR(255) COMMENT 'Por qué se sugiere (Ej: Por Explosión BOM)',
    FOREIGN KEY (rfq_id) REFERENCES cmp_rfq_campanas(id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS cmp_rfq_cotizaciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rfq_detalle_id INT NOT NULL,
    proveedor_id INT NOT NULL,
    precio_ofrecido DECIMAL(15, 4) NOT NULL,
    moneda VARCHAR(3) DEFAULT 'ARS',
    fecha_entrega DATE NOT NULL,
    adjudicada TINYINT(1) DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (rfq_detalle_id) REFERENCES cmp_rfq_detalles(id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
-- ============================================================
-- MSAC Fase 1.5: Consignaciones y Fazón (Stock de Terceros)
-- ============================================================
-- Transformamos los depósitos: ahora un depósito puede no ser nuestro.
-- Por ej: Un taller externo de Fazón es un "Depósito" tipo 'FAZON_TERCERO'
-- asignado al tercero_id del Taller.
ALTER TABLE stk_depositos
ADD COLUMN IF NOT EXISTS tipo_propiedad ENUM(
        'PROPIO',
        'FAZON_TERCERO',
        'CONSIGNACION_PROVEEDOR',
        'CONSIGNACION_CLIENTE'
    ) DEFAULT 'PROPIO',
    ADD COLUMN IF NOT EXISTS tercero_id INT NULL COMMENT 'ID del proveedor/cliente dueño fisico o legal del deposito';
-- Tabla para que los terceros (Taller, Consignatario) reporten qué usaron/vendieron.
-- Esto detona automáticamente la Generación de la Factura.
CREATE TABLE IF NOT EXISTS stk_liquidaciones_consignacion (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    tercero_id INT NOT NULL,
    deposito_id INT NOT NULL,
    fecha_reporte DATETIME DEFAULT CURRENT_TIMESTAMP,
    estado ENUM('PENDIENTE_FACTURACION', 'FACTURADO') DEFAULT 'PENDIENTE_FACTURACION',
    comprobante_id INT NULL COMMENT 'Factura resultante generada',
    user_id INT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_liq_ent (enterprise_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS stk_liquidaciones_consignacion_det (
    id INT AUTO_INCREMENT PRIMARY KEY,
    liquidacion_id INT NOT NULL,
    articulo_id INT NOT NULL,
    cantidad_consumida DECIMAL(15, 4) NOT NULL,
    precio_pactado DECIMAL(15, 4) NOT NULL,
    FOREIGN KEY (liquidacion_id) REFERENCES stk_liquidaciones_consignacion(id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;