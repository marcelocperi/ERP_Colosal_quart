--- ==============================================================================
-- ERP SCHEMA - MODULOS DE GESTION (ARGENTINA)
-- ==============================================================================
-- 1. TABLA MAESTRA DE TERCEROS (Clientes y Proveedores Unificados)
CREATE TABLE erp_terceros (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    nombre VARCHAR(200) NOT NULL,
    cuit VARCHAR(20),
    -- CUIT/CUIL sin guiones
    tipo_responsable VARCHAR(50),
    -- Resp. Inscripto, Monotributo, Exento, Consumidor Final
    -- Direccion se mueve a tabla hija erp_direcciones
    observaciones TEXT,
    telefono VARCHAR(50),
    -- Telefono principal (legacy/rapido)
    email VARCHAR(100),
    -- Email principal (legacy/rapido)
    es_cliente BOOLEAN DEFAULT 0,
    es_proveedor BOOLEAN DEFAULT 0,
    activo BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ent_cuit (enterprise_id, cuit)
);
-- 1.1 DIRECCIONES (Sedes, Depósitos, Sucursales)
CREATE TABLE erp_direcciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tercero_id INT NOT NULL,
    etiqueta VARCHAR(100),
    -- Ej: Casa Central, Depósito Pilar
    calle VARCHAR(100),
    numero VARCHAR(20),
    piso VARCHAR(10),
    depto VARCHAR(10),
    municipio VARCHAR(100),
    localidad VARCHAR(100),
    provincia VARCHAR(100),
    pais VARCHAR(50) DEFAULT 'Argentina',
    cod_postal VARCHAR(20),
    es_fiscal BOOLEAN DEFAULT 0,
    es_entrega BOOLEAN DEFAULT 0,
    FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id) ON DELETE CASCADE
);
-- 1.2 CONTACTOS (Ventas, Compras, Tesorería)
CREATE TABLE erp_contactos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tercero_id INT NOT NULL,
    nombre VARCHAR(100),
    -- Persona de contacto
    puesto_id INT,
    tipo_contacto VARCHAR(50),
    -- Compras, Ventas, Tesorería, Gerencia
    telefono VARCHAR(50),
    email VARCHAR(100),
    FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id) ON DELETE CASCADE,
    FOREIGN KEY (puesto_id) REFERENCES erp_puestos(id) ON DELETE
    SET NULL
);
-- 1.3 SITUACION FISCAL (IIBB, Ganancias por Jurisdicción)
CREATE TABLE erp_datos_fiscales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tercero_id INT NOT NULL,
    impuesto VARCHAR(50),
    -- IIBB, GANANCIAS, TASAS MUNICIPALES
    jurisdiccion VARCHAR(100),
    -- CABA, BUENOS AIRES, CORDOBA (o "NACIONAL")
    condicion VARCHAR(50),
    -- Inscripto, Exento, CM (Convenio Multilateral)
    numero_inscripcion VARCHAR(50),
    fecha_vencimiento DATE,
    -- Para exenciones
    alicuota DECIMAL(5, 2),
    -- % de percepción/retención específica
    FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id) ON DELETE CASCADE
);
-- 2. CONTABILIDAD: PLAN DE CUENTAS
CREATE TABLE cont_plan_cuentas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    codigo VARCHAR(50) NOT NULL,
    -- Ej: 1.1.01.001
    nombre VARCHAR(200) NOT NULL,
    tipo VARCHAR(20),
    -- ACTIVO, PASIVO, PN, INGRESO, EGRESO
    imputable BOOLEAN DEFAULT 1,
    -- Si recibe asientos (Hoja) o es sumadora (Rama)
    padre_id INT,
    -- Jerarquía
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(enterprise_id, codigo)
);
-- 3. CONTABILIDAD: ASIENTOS (Cabecera)
CREATE TABLE cont_asientos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    fecha DATE NOT NULL,
    concepto VARCHAR(255),
    modulo_origen VARCHAR(20) DEFAULT 'MANUAL',
    -- VENTAS, COMPRAS, FONDOS, MANUAL
    comprobante_id INT,
    -- Link al ID de la factura/pago origen
    numero_asiento INT,
    -- Correlativo anual
    estado VARCHAR(20) DEFAULT 'CONFIRMADO',
    -- BORRADOR, CONFIRMADO, ANULADO
    user_id INT,
    -- Quien lo cargó
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 4. CONTABILIDAD: ASIENTOS (Detalle)
CREATE TABLE cont_asientos_detalle (
    id INT AUTO_INCREMENT PRIMARY KEY,
    asiento_id INT NOT NULL,
    cuenta_id INT NOT NULL,
    debe DECIMAL(15, 2) DEFAULT 0,
    haber DECIMAL(15, 2) DEFAULT 0,
    glosa VARCHAR(255),
    FOREIGN KEY (asiento_id) REFERENCES cont_asientos(id) ON DELETE CASCADE,
    FOREIGN KEY (cuenta_id) REFERENCES cont_plan_cuentas(id)
);
-- 5. COMPRAS Y VENTAS: COMPROBANTES (Facturas, Notas de Débito/Crédito)
CREATE TABLE erp_comprobantes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    modulo VARCHAR(10),
    -- 'VENTAS' o 'COMPRAS'
    tercero_id INT NOT NULL,
    tipo_comprobante VARCHAR(5),
    -- AFIP: 001 (Fac A), 006 (Fac B), 011 (Fac C), etc.
    punto_venta INT NOT NULL,
    numero INT NOT NULL,
    fecha_emision DATE NOT NULL,
    fecha_vencimiento DATE,
    importe_neto DECIMAL(15, 2) DEFAULT 0,
    importe_iva DECIMAL(15, 2) DEFAULT 0,
    importe_total DECIMAL(15, 2) DEFAULT 0,
    estado_pago VARCHAR(20) DEFAULT 'PENDIENTE',
    -- PENDIENTE, PARCIAL, PAGADO
    cae VARCHAR(50),
    -- Código de Autorización Electrónico AFIP
    vto_cae DATE,
    asiento_id INT,
    -- Link contable
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tercero_id) REFERENCES erp_terceros(id)
);
-- 6. FONDOS: CAJAS Y BANCOS
CREATE TABLE erp_cuentas_fondos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    -- Ej: Caja Principal, Banco Galicia CC
    tipo VARCHAR(20),
    -- EFECTIVO, BANCO, WALLET
    moneda VARCHAR(3) DEFAULT 'ARS',
    cbu_alias VARCHAR(100),
    saldo_actual DECIMAL(15, 2) DEFAULT 0,
    cuenta_contable_id INT,
    -- Link al plan de cuentas
    activo BOOLEAN DEFAULT 1
);
-- 7. FONDOS: MOVIMIENTOS DE TESORERIA (Cobranzas y Pagos)
CREATE TABLE erp_movimientos_fondos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    fecha DATE NOT NULL,
    tipo VARCHAR(10),
    -- INGRESO, EGRESO
    tercero_id INT,
    cuenta_fondo_id INT NOT NULL,
    importe DECIMAL(15, 2) NOT NULL,
    concepto VARCHAR(255),
    comprobante_asociado_id INT,
    -- Si paga una factura específica
    asiento_id INT,
    user_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (cuenta_fondo_id) REFERENCES erp_cuentas_fondos(id)
);
-- 8. GEOGRAFIA (Sincronizado con Georef Argentina)
CREATE TABLE sys_provincias (
    id VARCHAR(10) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    iso_id VARCHAR(10),
    centroide_lat DECIMAL(15, 12),
    centroide_lon DECIMAL(15, 12)
);
CREATE TABLE sys_departamentos (
    id VARCHAR(10) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    provincia_id VARCHAR(10) NOT NULL,
    centroide_lat DECIMAL(15, 12),
    centroide_lon DECIMAL(15, 12),
    FOREIGN KEY (provincia_id) REFERENCES sys_provincias(id)
);
CREATE TABLE sys_municipios (
    id VARCHAR(20) PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    nombre_completo VARCHAR(255),
    provincia_id VARCHAR(10) NOT NULL,
    centroide_lat DECIMAL(15, 12),
    centroide_lon DECIMAL(15, 12),
    categoria VARCHAR(100),
    FOREIGN KEY (provincia_id) REFERENCES sys_provincias(id)
);
CREATE TABLE sys_localidades (
    id VARCHAR(20) PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    provincia_id VARCHAR(10) NOT NULL,
    municipio_id VARCHAR(20),
    centroide_lat DECIMAL(15, 12),
    centroide_lon DECIMAL(15, 12),
    FOREIGN KEY (provincia_id) REFERENCES sys_provincias(id),
    FOREIGN KEY (municipio_id) REFERENCES sys_municipios(id)
);
-- ==============================================================================
-- DATOS DE INICIO (SEED DATA) - MODELO ARGENTINO
-- ==============================================================================
-- NOTA: Estos inserts asumen enterprise_id = 1. Para nuevas empresas, se deben replicar.
-- 1. PLAN DE CUENTAS BASE (Simplificado RT 8/9)
-- ACTIVO
INSERT INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        imputable,
        padre_id
    )
VALUES (0, '1', 'ACTIVO', 'ACTIVO', 0, NULL),
    (0, '1.1', 'CAJA Y BANCOS', 'ACTIVO', 0, 1),
    (0, '1.1.01', 'Caja', 'ACTIVO', 0, 2),
    (
        0,
        '1.1.01.001',
        'Caja Administración',
        'ACTIVO',
        1,
        3
    ),
    (0, '1.1.02', 'Bancos', 'ACTIVO', 0, 2),
    (
        0,
        '1.1.02.001',
        'Banco Nación c/c',
        'ACTIVO',
        1,
        5
    ),
    (0, '1.2', 'INVERSIONES', 'ACTIVO', 0, 1),
    (0, '1.3', 'CREDITOS POR VENTAS', 'ACTIVO', 0, 1),
    (
        0,
        '1.3.01',
        'Deudores por Ventas',
        'ACTIVO',
        1,
        8
    ),
    (0, '1.3.02', 'Deudores Morosos', 'ACTIVO', 1, 8),
    (0, '1.4', 'BIENES DE CAMBIO', 'ACTIVO', 0, 1),
    (
        0,
        '1.4.01',
        'Mercaderías de Reventa',
        'ACTIVO',
        1,
        11
    );
-- PASIVO
INSERT INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        imputable,
        padre_id
    )
VALUES (0, '2', 'PASIVO', 'PASIVO', 0, NULL),
    (0, '2.1', 'DEUDAS COMERCIALES', 'PASIVO', 0, 13),
    (0, '2.1.01', 'Proveedores', 'PASIVO', 1, 14),
    (0, '2.2', 'DEUDAS FISCALES', 'PASIVO', 0, 13),
    (
        0,
        '2.2.01',
        'IVA Débito Fiscal',
        'PASIVO',
        1,
        16
    ),
    (0, '2.2.02', 'IVA a Pagar', 'PASIVO', 1, 16),
    (0, '2.2.03', 'IIBB a Pagar', 'PASIVO', 1, 16);
-- PATRIMONIO NETO
INSERT INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        imputable,
        padre_id
    )
VALUES (0, '3', 'PATRIMONIO NETO', 'PN', 0, NULL),
    (0, '3.1', 'CAPITAL SOCIAL', 'PN', 1, 20),
    (0, '3.2', 'RESULTADOS ACUMULADOS', 'PN', 1, 20);
-- RESULTADOS (INGRESOS)
INSERT INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        imputable,
        padre_id
    )
VALUES (0, '4', 'INGRESOS', 'INGRESO', 0, NULL),
    (0, '4.1', 'VENTAS', 'INGRESO', 1, 23);
-- RESULTADOS (EGRESOS)
INSERT INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        imputable,
        padre_id
    )
VALUES (0, '5', 'EGRESOS', 'EGRESO', 0, NULL),
    (0, '5.1', 'COSTOS', 'EGRESO', 0, 25),
    (0, '5.1.01', 'CMV', 'EGRESO', 1, 26),
    (
        0,
        '5.2',
        'GASTOS ADMINISTRACION',
        'EGRESO',
        0,
        25
    ),
    (
        0,
        '5.2.01',
        'Sueldos y Jornales',
        'EGRESO',
        1,
        28
    ),
    (0, '5.2.02', 'Alquileres', 'EGRESO', 1, 28),
    (
        0,
        '5.3',
        'GASTOS COMERCIALIZACION',
        'EGRESO',
        0,
        25
    ),
    (0, '5.3.01', 'Publicidad', 'EGRESO', 1, 31),
    (
        0,
        '5.3.02',
        'Impuesto a los Ingresos Brutos',
        'EGRESO',
        1,
        31
    ),
    (0, '5.4', 'GASTOS FINANCIEROS', 'EGRESO', 0, 25),
    (
        0,
        '5.4.01',
        'Intereses Perdidos',
        'EGRESO',
        1,
        34
    ),
    (0, '5.4.02', 'Gastos Bancarios', 'EGRESO', 1, 34);
-- 2. CUENTAS DE FONDOS INICIALES
INSERT INTO erp_cuentas_fondos (
        enterprise_id,
        nombre,
        tipo,
        moneda,
        saldo_actual,
        cuenta_contable_id
    )
VALUES (0, 'Caja Principal', 'EFECTIVO', 'ARS', 0.00, 4),
    -- Link a 1.1.01.001
    (0, 'Banco Galicia CC', 'BANCO', 'ARS', 0.00, 6);
-- Link a 1.1.02.001