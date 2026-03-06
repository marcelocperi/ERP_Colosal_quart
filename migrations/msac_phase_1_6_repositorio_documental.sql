-- ============================================================
-- MSAC Fase 1.6: Control de Calidad, Ensayos y Repositorio Documental
-- ============================================================
-- 1. Ampliacion de Tipos de Costos para Produccion Industrial
-- Agregamos Calidad, Ensayos y Certificación a los Enum de costos.
ALTER TABLE cmp_articulos_costos_indirectos
MODIFY COLUMN tipo_gasto ENUM(
        'MANO_DE_OBRA',
        'ENERGIA',
        'AMORTIZACION',
        'FLETE_INTERNO',
        'LOGISTICA',
        'CERTIFICACION',
        'CONTROL_CALIDAD',
        'ENSAYOS',
        'OTROS'
    ) NOT NULL DEFAULT 'OTROS';
ALTER TABLE cmp_overhead_templates_detalle
MODIFY COLUMN tipo_gasto ENUM(
        'MANO_DE_OBRA',
        'ENERGIA',
        'AMORTIZACION',
        'FLETE_INTERNO',
        'LOGISTICA',
        'CERTIFICACION',
        'CONTROL_CALIDAD',
        'ENSAYOS',
        'OTROS'
    ) NOT NULL;
ALTER TABLE cmp_overhead_cuenta_contable
MODIFY COLUMN tipo_gasto ENUM(
        'MANO_DE_OBRA',
        'ENERGIA',
        'AMORTIZACION',
        'FLETE_INTERNO',
        'LOGISTICA',
        'CERTIFICACION',
        'CONTROL_CALIDAD',
        'ENSAYOS',
        'OTROS'
    ) NOT NULL;
-- Sembrar las cuentas contables para los nuevos tipos (FACPCE)
INSERT IGNORE INTO cont_plan_cuentas (
        enterprise_id,
        codigo,
        nombre,
        tipo,
        naturaleza,
        activo
    )
VALUES (
        1,
        '5.4.05',
        'Gastos de Control de Calidad y Laboratorio',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        1,
        '5.4.06',
        'Ensayos Técnicos e I+D',
        'DETALLE',
        'DEUDORA',
        1
    ),
    (
        1,
        '5.4.07',
        'Certificaciones y Habilitaciones Industriales',
        'DETALLE',
        'DEUDORA',
        1
    );
INSERT IGNORE INTO cmp_overhead_cuenta_contable (enterprise_id, tipo_gasto, cuenta_codigo)
VALUES (1, 'CONTROL_CALIDAD', '5.4.05'),
    (1, 'ENSAYOS', '5.4.06'),
    (1, 'CERTIFICACION', '5.4.07');
-- ============================================================
-- 2. REPOSITORIO DOCUMENTAL TÉCNICO Y LEGAL
-- Sistema de gestión documental para Fazón, Artículos y Proyectos.
-- ============================================================
CREATE TABLE IF NOT EXISTS sys_documentos_adjuntos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    -- Entidad Polimorfica: ¿A qué está adjunto este documento?
    entidad_tipo ENUM(
        'ARTICULO',
        -- Fichas tecnicas, Bromatologia, RNE/RNPA
        'PROVEEDOR',
        -- Contratos Fazón, NDA, Seguros
        'ORDEN_COMPRA',
        -- Documentos comerciales
        'PROYECTO_PRODUCCION',
        -- Legajo técnico de desarrollo de un producto
        'CONTROL_CALIDAD' -- Protocolos de análisis de un lote
    ) NOT NULL,
    entidad_id INT NOT NULL COMMENT 'ID del Articulo, Proveedor, Proyecto, etc.',
    -- Metadatos del Documento
    tipo_documento VARCHAR(50) NOT NULL COMMENT 'Ej: PLANO, CERTIFICADO, CONTRATO, RNE, MSDS',
    nombre_archivo VARCHAR(255) NOT NULL,
    ruta_almacenamiento VARCHAR(500) NOT NULL COMMENT 'Ruta local o URL S3',
    fecha_emision DATE NULL,
    fecha_vencimiento DATE NULL COMMENT 'Para alarmas si vence una certificacion o RNE',
    -- Control de Estados
    estado ENUM('VIGENTE', 'VENCIDO', 'SUSTITUIDO') DEFAULT 'VIGENTE',
    version VARCHAR(20) DEFAULT '1.0',
    notas TEXT,
    -- Auditoria
    user_id INT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_entidad_repo (enterprise_id, entidad_tipo, entidad_id),
    KEY idx_vencimientos (enterprise_id, fecha_vencimiento)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
-- Generamos un módulo "Proyectos de Desarrollo" ligero para Fazón/Nuevos Productos
CREATE TABLE IF NOT EXISTS prd_proyectos_desarrollo (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    codigo_proyecto VARCHAR(50) NOT NULL,
    nombre VARCHAR(150) NOT NULL,
    descripcion TEXT,
    articulo_objetivo_id INT NULL COMMENT 'ID del articulo que se busca crear/mejorar',
    estado ENUM(
        'EVALUACION',
        'I_D',
        'HOMOLOGACION_LEGAL',
        'APROBADO',
        'DESCARTADO'
    ) DEFAULT 'EVALUACION',
    fecha_inicio DATE NOT NULL,
    fecha_fin_estimada DATE NULL,
    presupuesto_estimado DECIMAL(15, 4) DEFAULT 0,
    -- Auditoria
    user_id INT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_proyecto_ent (enterprise_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;