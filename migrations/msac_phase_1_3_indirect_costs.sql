CREATE TABLE IF NOT EXISTS cmp_articulos_costos_indirectos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    articulo_id INT NOT NULL,
    tipo_gasto ENUM(
        'MANO_DE_OBRA',
        'ENERGIA',
        'AMORTIZACION',
        'FLETE_INTERNO',
        'OTROS'
    ) NOT NULL DEFAULT 'OTROS',
    descripcion VARCHAR(200) NOT NULL,
    base_calculo ENUM('UNIDAD', 'BATCH') NOT NULL DEFAULT 'UNIDAD',
    cantidad_batch INT NOT NULL DEFAULT 1,
    monto_estimado DECIMAL(14, 4) NOT NULL DEFAULT 0.0000,
    porcentaje_margen_esperado DECIMAL(5, 2) NOT NULL DEFAULT 20.00,
    activo TINYINT(1) NOT NULL DEFAULT 1,
    user_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id_update INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_empresa_articulo (enterprise_id, articulo_id),
    INDEX idx_tipo_gasto (tipo_gasto)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
CREATE TABLE IF NOT EXISTS cmp_overhead_templates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enterprise_id INT NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    descripcion TEXT,
    activo TINYINT(1) NOT NULL DEFAULT 1,
    user_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
CREATE TABLE IF NOT EXISTS cmp_overhead_templates_detalle (
    id INT AUTO_INCREMENT PRIMARY KEY,
    template_id INT NOT NULL,
    enterprise_id INT NOT NULL,
    tipo_gasto ENUM(
        'MANO_DE_OBRA',
        'ENERGIA',
        'AMORTIZACION',
        'FLETE_INTERNO',
        'OTROS'
    ) NOT NULL,
    descripcion VARCHAR(200) NOT NULL,
    monto_estimado DECIMAL(14, 4) NOT NULL DEFAULT 0.0000,
    base_calculo ENUM('UNIDAD', 'BATCH') NOT NULL DEFAULT 'UNIDAD',
    cantidad_batch INT NOT NULL DEFAULT 1,
    user_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (template_id) REFERENCES cmp_overhead_templates(id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;