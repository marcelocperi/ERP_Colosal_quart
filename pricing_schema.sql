-- ==============================================================================
-- PRICING & PRICE LISTS MODULE - REFINED SCHEMA
-- ==============================================================================
-- 1. Métodos de Costeo (Diccionario)
CREATE TABLE IF NOT EXISTS stk_metodos_costeo (
    id INT AUTO_INCREMENT PRIMARY KEY,
    codigo VARCHAR(20) UNIQUE NOT NULL,
    -- e.g. 'FIFO', 'LIFO', 'WAC', 'ID_SPEC', 'RETAIL', 'CUSTOM'
    nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 2. Fórmulas Customizadas (Para el método CUSTOM)
CREATE TABLE IF NOT EXISTS stk_pricing_formulas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    formula_expression TEXT NOT NULL,
    -- Ej: "(COSTO_PPP * 0.8) + (COSTO_IMPORT * 0.2)"
    descripcion TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 3. Listas de Precios
CREATE TABLE IF NOT EXISTS stk_listas_precios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    moneda VARCHAR(3) DEFAULT 'ARS',
    descripcion TEXT,
    activo BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 4. Reglas de Pricing por Naturaleza / Método
CREATE TABLE IF NOT EXISTS stk_pricing_reglas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    lista_precio_id INT NOT NULL,
    naturaleza VARCHAR(50) NOT NULL,
    -- 'PRODUCTO', 'SERVICIO', etc.
    metodo_costo_id INT NOT NULL,
    -- FK a stk_metodos_costeo
    formula_id INT,
    -- FK a stk_pricing_formulas (si el método es CUSTOM)
    coeficiente_markup DECIMAL(10, 4) DEFAULT 1.0000,
    prioridad INT DEFAULT 0,
    activo BOOLEAN DEFAULT 1,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (lista_precio_id) REFERENCES stk_listas_precios(id),
    FOREIGN KEY (metodo_costo_id) REFERENCES stk_metodos_costeo(id),
    FOREIGN KEY (formula_id) REFERENCES stk_pricing_formulas(id)
);
-- 5. Precios de Artículos (Histórico y Desacoplado)
CREATE TABLE IF NOT EXISTS stk_articulos_precios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    articulo_id INT NOT NULL,
    lista_precio_id INT NOT NULL,
    regla_id INT,
    costo_base_snapshot DECIMAL(15, 4),
    precio_final DECIMAL(15, 2) NOT NULL,
    fecha_inicio_vigencia DATETIME NOT NULL,
    fecha_fin_vigencia DATETIME,
    es_manual BOOLEAN DEFAULT 0,
    user_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (articulo_id) REFERENCES stk_articulos(id),
    FOREIGN KEY (lista_precio_id) REFERENCES stk_listas_precios(id),
    FOREIGN KEY (regla_id) REFERENCES stk_pricing_reglas(id),
    INDEX idx_vigencia (
        articulo_id,
        lista_precio_id,
        fecha_inicio_vigencia
    )
);
-- Propuestas de Precio para Aprobación (Workflow)
CREATE TABLE IF NOT EXISTS stk_pricing_propuestas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    lista_id INT NOT NULL,
    articulo_id INT NOT NULL,
    costo_base_snapshot DECIMAL(19, 4) NOT NULL,
    precio_sugerido DECIMAL(19, 4) NOT NULL,
    markup_aplicado DECIMAL(10, 4),
    metodo_costeo_id INT,
    estado ENUM('PENDIENTE', 'APROBADO', 'RECHAZADO') DEFAULT 'PENDIENTE',
    motivo TEXT,
    usuario_id_propuesta INT,
    usuario_id_aprobacion INT,
    fecha_propuesta DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_aprobacion DATETIME,
    FOREIGN KEY (lista_id) REFERENCES stk_listas_precios(id),
    FOREIGN KEY (articulo_id) REFERENCES stk_articulos(id)
);
-- POBLAR MÉTODOS DE COSTEO
INSERT IGNORE INTO stk_metodos_costeo (codigo, nombre, descripcion)
VALUES (
        'PEPS',
        'FIFO / PEPS',
        'Primero en entrar, primero en salir'
    ),
    (
        'UEPS',
        'LIFO / UEPS',
        'Último en entrar, primero en salir'
    ),
    (
        'WAC',
        'Costo Promedio Ponderado (WAC)',
        'Weighted Average Cost'
    ),
    (
        'ID_SPEC',
        'Identificación Específica',
        'Costo real de cada artículo individualmente'
    ),
    (
        'RETAIL',
        'Inventario Minorista',
        'Ratio de costo-venta para minoristas'
    ),
    (
        'REPOSICION',
        'Costo de Reposición',
        'Costo actual de volver a comprar el bien'
    ),
    (
        'IMPORTACION',
        'Costo Importación',
        'Costo unitario de importación calculado (CUI)'
    ),
    (
        'CUSTOM',
        'Personalizado (Fórmula)',
        'Cálculo basado en una fórmula definible'
    );
-- POBLAR LISTA BASE
INSERT IGNORE INTO stk_listas_precios (enterprise_id, nombre, descripcion)
VALUES (
        0,
        'Lista General',
        'Lista de precios base para venta al público'
    );