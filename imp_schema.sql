-- ==============================================================================
-- MÓDULO DE IMPORTACIONES — ETAPA 1
-- "Base de Datos Internacional + Divisas"
-- ==============================================================================
-- Ejecutar con: SOURCE imp_schema.sql
-- Impacto: SOLO ADICIÓN. No modifica ni elimina nada existente.
-- ==============================================================================
-- ------------------------------------------------------------------------------
-- 1. EXTENSIÓN DE erp_terceros — Soporte para Proveedores Internacionales
--    Agrega columnas opcionales; CUIT puede ser nulo para extranjeros.
-- ------------------------------------------------------------------------------
ALTER TABLE erp_terceros
ADD COLUMN IF NOT EXISTS es_proveedor_extranjero TINYINT(1) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pais_origen VARCHAR(100) DEFAULT NULL,
    -- 'CHINA', 'BRASIL', 'USA', etc.
ADD COLUMN IF NOT EXISTS codigo_pais_iso CHAR(2) DEFAULT NULL,
    -- ISO 3166-1: CN, BR, US, etc.
ADD COLUMN IF NOT EXISTS identificador_fiscal VARCHAR(60) DEFAULT NULL,
    -- Tax ID / VAT extranjero
ADD COLUMN IF NOT EXISTS codigo_swift VARCHAR(20) DEFAULT NULL,
    -- BIC/SWIFT del banco
ADD COLUMN IF NOT EXISTS moneda_operacion CHAR(3) DEFAULT 'ARS',
    -- USD, EUR, BRL, ARS
ADD COLUMN IF NOT EXISTS web VARCHAR(200) DEFAULT NULL;
-- Permitir CUIT nulo para proveedores extranjeros (era NOT NULL en algunos motores)
-- NOTA: Solo aplica si la columna tiene restricción. El sistema ya usa VARCHAR para CUIT.
-- No se modifica la estructura de cuit, ya que al insertarse con validacion por app.
-- ------------------------------------------------------------------------------
-- 2. TABLA DE TIPOS DE CAMBIO HISTÓRICOS
--    Fuente: BCRA API (https://api.bcra.gob.ar)
--    Se actualiza diariamente con el cron job existente.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fin_tipos_cambio (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL DEFAULT 0,
    -- 0 = global/sistema, N = empresa específica
    fecha DATE NOT NULL,
    moneda CHAR(3) NOT NULL,
    -- USD, EUR, BRL, JPY, GBP, etc.
    tipo VARCHAR(30) NOT NULL,
    -- 'OFICIAL_VENDEDOR', 'OFICIAL_COMPRADOR', 'MEP', 'CCL', 'BCRA_MINORISTA'
    valor DECIMAL(18, 6) NOT NULL,
    -- Cotización en ARS por 1 unidad de moneda extranjera
    fuente VARCHAR(50) DEFAULT 'BCRA',
    -- 'BCRA', 'MANUAL', 'DOLARITO_API', etc.
    user_id INT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tipo_cambio (enterprise_id, fecha, moneda, tipo)
);
-- Insertar valores iniciales de referencia (actualizables por cron)
INSERT IGNORE INTO fin_tipos_cambio (
        enterprise_id,
        fecha,
        moneda,
        tipo,
        valor,
        fuente
    )
VALUES (
        0,
        CURDATE(),
        'USD',
        'OFICIAL_VENDEDOR',
        1050.00,
        'SEED'
    ),
    (
        0,
        CURDATE(),
        'USD',
        'OFICIAL_COMPRADOR',
        1040.00,
        'SEED'
    ),
    (0, CURDATE(), 'USD', 'MEP', 1200.00, 'SEED'),
    (
        0,
        CURDATE(),
        'EUR',
        'OFICIAL_VENDEDOR',
        1130.00,
        'SEED'
    ),
    (
        0,
        CURDATE(),
        'BRL',
        'OFICIAL_VENDEDOR',
        200.00,
        'SEED'
    );
-- ------------------------------------------------------------------------------
-- 3. EXTENSIÓN DE cmp_ordenes_compra — Campos de Importación
--    Agrega campos opcionales (NULL = compra local normal)
-- ------------------------------------------------------------------------------
ALTER TABLE cmp_ordenes_compra
ADD COLUMN IF NOT EXISTS es_importacion TINYINT(1) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS moneda CHAR(3) DEFAULT 'ARS',
    ADD COLUMN IF NOT EXISTS tipo_cambio_id INT DEFAULT NULL,
    -- FK a fin_tipos_cambio
ADD COLUMN IF NOT EXISTS tipo_cambio_valor DECIMAL(18, 6) DEFAULT NULL,
    -- Snapshot al momento
ADD COLUMN IF NOT EXISTS incoterm VARCHAR(10) DEFAULT NULL,
    -- EXW, FOB, CIF, DDP, etc.
ADD COLUMN IF NOT EXISTS pais_origen VARCHAR(100) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS puerto_embarque VARCHAR(150) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS puerto_destino VARCHAR(150) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS total_estimado_usd DECIMAL(18, 2) DEFAULT NULL;
-- ------------------------------------------------------------------------------
-- 4. TABLA DE DOCUMENTOS DE IMPORTACIÓN
--    Registra cada documento del circuito: Proforma, Invoice, BL, Packing List
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imp_documentos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    orden_compra_id INT DEFAULT NULL,
    -- FK a cmp_ordenes_compra (opcional si es independiente)
    tipo_documento VARCHAR(40) NOT NULL,
    -- 'PROFORMA', 'COMMERCIAL_INVOICE', 'PACKING_LIST', 'BL', 'AWB', 'CERTIFICADO_ORIGEN', 'OTRO'
    numero_documento VARCHAR(80) DEFAULT NULL,
    -- Nro. del doc. del proveedor
    fecha_documento DATE DEFAULT NULL,
    fecha_vencimiento DATE DEFAULT NULL,
    -- Para cartas de crédito, etc.
    monto DECIMAL(18, 2) DEFAULT NULL,
    moneda CHAR(3) DEFAULT 'USD',
    proveedor_id INT DEFAULT NULL,
    -- FK a erp_terceros
    descripcion TEXT DEFAULT NULL,
    archivo_path VARCHAR(300) DEFAULT NULL,
    -- Ruta al PDF subido
    estado VARCHAR(30) DEFAULT 'PENDIENTE',
    -- 'PENDIENTE', 'RECIBIDO', 'VERIFICADO', 'OBSERVADO'
    user_id INT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
-- ------------------------------------------------------------------------------
-- 5. TABLA DE CARGOS DE IMPORTACIÓN (preparación para Etapa 2)
--    Solo la estructura, sin lógica de negocio aún.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS imp_cargos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    orden_compra_id INT NOT NULL,
    tipo_cargo VARCHAR(50) NOT NULL,
    -- 'FLETE_INTERNACIONAL', 'SEGURO', 'DERECHOS_IMPORTACION', 'TASA_ESTADISTICA', 'HONORARIOS_DESPACHANTE', 'FLETE_INTERNO', 'ALMACENAMIENTO', 'OTRO'
    descripcion VARCHAR(200) DEFAULT NULL,
    proveedor_id INT DEFAULT NULL,
    -- Quién cobra el cargo (flete, aduana, despachante)
    monto_orig DECIMAL(18, 2) DEFAULT NULL,
    -- Monto en moneda original
    moneda_orig CHAR(3) DEFAULT 'USD',
    tipo_cambio DECIMAL(18, 6) DEFAULT NULL,
    -- TC al momento del registro
    monto_ars DECIMAL(18, 2) DEFAULT NULL,
    -- Monto convertido a ARS
    comprobante_id INT DEFAULT NULL,
    -- FK a erp_comprobantes si se facturó
    fecha DATE DEFAULT NULL,
    user_id INT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- ------------------------------------------------------------------------------
-- 6. VISTA ÚTIL: Tipo de cambio más reciente por moneda
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_tipo_cambio_vigente AS
SELECT t1.*
FROM fin_tipos_cambio t1
    INNER JOIN (
        SELECT moneda,
            tipo,
            MAX(fecha) as max_fecha
        FROM fin_tipos_cambio
        WHERE enterprise_id = 0
        GROUP BY moneda,
            tipo
    ) t2 ON t1.moneda = t2.moneda
    AND t1.tipo = t2.tipo
    AND t1.fecha = t2.max_fecha
WHERE t1.enterprise_id = 0;
-- ------------------------------------------------------------------------------
-- 7. ÍNDICES para performance de consultas frecuentes
-- ------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fin_tc_fecha_moneda ON fin_tipos_cambio (fecha, moneda);
CREATE INDEX IF NOT EXISTS idx_imp_docs_orden ON imp_documentos (orden_compra_id);
CREATE INDEX IF NOT EXISTS idx_imp_docs_tipo_estado ON imp_documentos (tipo_documento, estado);
CREATE INDEX IF NOT EXISTS idx_imp_cargos_orden ON imp_cargos (orden_compra_id);
-- ==============================================================================
-- FIN ETAPA 1 — Migración completada
-- ==============================================================================