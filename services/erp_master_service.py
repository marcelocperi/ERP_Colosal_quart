from database import get_db_cursor
import logging

logger = logging.getLogger(__name__)

class ErpMasterService:
    @staticmethod
    def initialize_db():
        """Inicializa las tablas maestras del ERP."""
        try:
            with get_db_cursor() as cursor:
                # Tabla de Puestos
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS erp_puestos (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        enterprise_id INT NOT NULL,
                        nombre VARCHAR(100) NOT NULL,
                        area VARCHAR(50), -- COMPRAS, VENTAS, GENERAL
                        activo BOOLEAN DEFAULT 1,
                        INDEX idx_ent_area (enterprise_id, area)
                    )
                """)
                
                # Seed básico si no hay datos
                cursor.execute("SELECT COUNT(*) FROM erp_puestos")
                if cursor.fetchone()[0] == 0:
                    logger.info("Sembrando puestos iniciales...")
                    puestos = [
                        (1, 'Gerente de Compras', 'COMPRAS'),
                        (1, 'Jefe de Compras', 'COMPRAS'),
                        (1, 'Comprador Senior', 'COMPRAS'),
                        (1, 'Comprador Junior', 'COMPRAS'),
                        (1, 'Analista de Cotizaciones', 'COMPRAS'),
                        (1, 'Gerente Comercial', 'VENTAS'),
                        (1, 'Jefe de Ventas', 'VENTAS'),
                        (1, 'Ejecutivo de Cuentas', 'VENTAS'),
                        (1, 'Vendedor Senior', 'VENTAS'),
                        (1, 'Responsable de Facturación', 'VENTAS'),
                        (1, 'Director General', 'GENERAL'),
                        (1, 'Gerente Administrativo', 'GENERAL'),
                        (1, 'Jefe de Tesorería', 'GENERAL'),
                        (1, 'Analista de Cuentas a Pagar', 'GENERAL'),
                        (1, 'Analista de Cuentas a Cobrar', 'GENERAL')
                    ]
                    cursor.executemany("INSERT INTO erp_puestos (enterprise_id, nombre, area) VALUES (%s, %s, %s)", puestos)
            logger.info("Tablas maestras ERP inicializadas.")
        except Exception as e:
            logger.error(f"Error inicializando DB ERP: {e}")

    @staticmethod
    def get_puestos(enterprise_id, area=None):
        with get_db_cursor(dictionary=True) as cursor:
            query = "SELECT * FROM erp_puestos WHERE enterprise_id = %s AND activo = 1"
            params = [enterprise_id]
            if area:
                query += " AND area = %s"
                params.append(area)
            query += " ORDER BY nombre"
            cursor.execute(query, params)
            return cursor.fetchall()
