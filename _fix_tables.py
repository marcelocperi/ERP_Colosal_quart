from database import get_db_cursor

stmts = [
    """CREATE TABLE IF NOT EXISTS prd_proyectos_desarrollo (
        id INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id INT NOT NULL,
        codigo_proyecto VARCHAR(50) NOT NULL,
        nombre VARCHAR(150) NOT NULL,
        descripcion TEXT,
        articulo_objetivo_id INT NULL,
        estado ENUM('EVALUACION', 'I_D', 'HOMOLOGACION_LEGAL', 'APROBADO', 'DESCARTADO') DEFAULT 'EVALUACION',
        fecha_inicio DATE NOT NULL,
        fecha_fin_estimada DATE NULL,
        presupuesto_estimado DECIMAL(15, 4) DEFAULT 0,
        user_id INT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_proyecto_ent (enterprise_id)
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;""",
    
    """CREATE TABLE IF NOT EXISTS sys_documentos_adjuntos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id INT NOT NULL,
        entidad_tipo ENUM('ARTICULO', 'PROVEEDOR', 'ORDEN_COMPRA', 'PROYECTO_PRODUCCION', 'CONTROL_CALIDAD') NOT NULL,
        entidad_id INT NOT NULL,
        tipo_documento VARCHAR(50) NOT NULL,
        nombre_archivo VARCHAR(255) NOT NULL,
        ruta_almacenamiento VARCHAR(500) NOT NULL,
        fecha_emision DATE NULL,
        fecha_vencimiento DATE NULL,
        estado ENUM('VIGENTE', 'VENCIDO', 'SUSTITUIDO') DEFAULT 'VIGENTE',
        version VARCHAR(20) DEFAULT '1.0',
        notas TEXT,
        user_id INT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_entidad_repo (enterprise_id, entidad_tipo, entidad_id)
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;""",

    """CREATE TABLE IF NOT EXISTS cmp_rfq_campanas (
        id INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id INT NOT NULL,
        fecha_emision DATETIME DEFAULT CURRENT_TIMESTAMP,
        fecha_cierre DATETIME NOT NULL,
        estado ENUM('BORRADOR', 'ENVIADA', 'CERRADA', 'ADJUDICADA') DEFAULT 'BORRADOR',
        articulo_objetivo_id INT NULL,
        cantidad_objetivo DECIMAL(15, 4) NULL,
        user_id INT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_rfq_ent (enterprise_id)
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;""",

    """CREATE TABLE IF NOT EXISTS cmp_rfq_detalles (
        id INT AUTO_INCREMENT PRIMARY KEY,
        rfq_id INT NOT NULL,
        articulo_insumo_id INT NOT NULL,
        cantidad_requerida DECIMAL(15, 4) NOT NULL,
        sugerencia_origen VARCHAR(255),
        FOREIGN KEY (rfq_id) REFERENCES cmp_rfq_campanas(id) ON DELETE CASCADE
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;"""
]

with get_db_cursor() as cursor:
    for sql in stmts:
        try:
            print(f"Running: {sql[:50]}...")
            cursor.execute(sql)
            print("Done.")
        except Exception as e:
            print(f"FAILED: {e}")
