from database import get_db_cursor

ddl = [
    """
    CREATE TABLE IF NOT EXISTS stk_devoluciones_solicitudes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        enterprise_id INT NOT NULL,
        tercero_id INT NOT NULL,
        comprobante_origen_id INT NOT NULL,
        fecha_solicitud DATETIME DEFAULT CURRENT_TIMESTAMP,
        estado ENUM('PENDIENTE', 'RECIBIDO_PARCIAL', 'RECIBIDO_TOTAL', 'PROCESADO', 'CANCELADO') DEFAULT 'PENDIENTE',
        deposito_destino_id INT,
        logistica_id INT,
        observaciones TEXT,
        user_id_solicita INT,
        user_id_logistica INT,
        condicion_devolucion_id INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS stk_devoluciones_solicitudes_det (
        id INT AUTO_INCREMENT PRIMARY KEY,
        solicitud_id INT NOT NULL,
        articulo_id INT NOT NULL,
        cantidad_solicitada DECIMAL(15,4) NOT NULL,
        cantidad_recibida DECIMAL(15,4) DEFAULT 0,
        precio_unitario DECIMAL(15,2) NOT NULL,
        alicuota_iva DECIMAL(5,2) NOT NULL,
        FOREIGN KEY (solicitud_id) REFERENCES stk_devoluciones_solicitudes(id) ON DELETE CASCADE
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS fin_devoluciones_valores (
        id INT AUTO_INCREMENT PRIMARY KEY,
        solicitud_id INT NOT NULL,
        medio_pago_id INT NOT NULL,
        importe DECIMAL(15,2) NOT NULL,
        observaciones VARCHAR(255),
        FOREIGN KEY (solicitud_id) REFERENCES stk_devoluciones_solicitudes(id) ON DELETE CASCADE
    ) ENGINE=InnoDB;
    """
]

with get_db_cursor() as cursor:
    for sql in ddl:
        print(f"Executing: {sql[:50]}...")
        cursor.execute(sql)
    print("Migration successful.")
