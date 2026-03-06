from database import get_db_cursor

with get_db_cursor() as cursor:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS erp_areas (
            id INT AUTO_INCREMENT PRIMARY KEY,
            enterprise_id INT NOT NULL DEFAULT 0,
            nombre VARCHAR(100) NOT NULL,
            color VARCHAR(20) DEFAULT 'secondary',
            icono VARCHAR(50) DEFAULT 'fa-building',
            activo TINYINT(1) DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_area_empresa (enterprise_id, nombre)
        )
    """)

    # Todas las áreas definidas en el sistema (una por categoría de menú + transversales)
    AREAS = [
        ('BIBLIOTECA',     'primary',   'fa-book-open'),
        ('PRODUCCION',     'warning',   'fa-industry'),
        ('CONTABILIDAD',   'secondary', 'fa-calculator'),
        ('COMPRAS',        'info',      'fa-shopping-cart'),
        ('VENTAS',         'success',   'fa-cash-register'),
        ('PRECIOS',        'success',   'fa-tags'),
        ('FONDOS',         'danger',    'fa-money-bill-wave'),
        ('COBRANZAS',      'info',      'fa-hand-holding-usd'),
        ('STOCK',          'warning',   'fa-boxes'),
        ('CONFIGURACION',  'secondary', 'fa-cog'),
        ('AUDITORIA',      'dark',      'fa-chart-line'),
        ('UTILITARIOS',    'info',      'fa-tools'),
        ('SISTEMA',        'danger',    'fa-crown'),
        ('GENERAL',        'secondary', 'fa-building'),   # comodín para todos los módulos
        ('RRHH',           'teal',      'fa-users-tie'),  # futuro módulo
        ('IMPUESTOS',      'orange',    'fa-percent'),    # futuro módulo
    ]

    for nombre, color, icono in AREAS:
        cursor.execute(
            "INSERT IGNORE INTO erp_areas (enterprise_id, nombre, color, icono) VALUES (0, %s, %s, %s)",
            (nombre, color, icono)
        )

print(f"Tabla erp_areas creada con {len(AREAS)} áreas sembradas OK")
