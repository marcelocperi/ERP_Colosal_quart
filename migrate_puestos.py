import mariadb
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate():
    try:
        conn = mariadb.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            user=os.getenv('MYSQL_USER', 'root'),
            password=os.getenv('MYSQL_PASSWORD', ''),
            database=os.getenv('MYSQL_DATABASE', 'biblioteca_mcp'),
            port=int(os.getenv('MYSQL_PORT', 3306))
        )
        cursor = conn.cursor()

        # Create Table
        logger.info("Creando tabla erp_puestos...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS erp_puestos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                nombre VARCHAR(100) NOT NULL,
                modulo VARCHAR(50), -- COMPRAS, VENTAS, GENERAL
                activo BOOLEAN DEFAULT 1,
                INDEX idx_ent_puesto (enterprise_id, modulo)
            )
        """)

        # Check if already seeded
        cursor.execute("SELECT COUNT(*) FROM erp_puestos WHERE enterprise_id = 1")
        count = cursor.fetchone()[0]
        if count == 0:
            logger.info("Sembrando puestos clave (Argentina)...")
            puestos = [
                (1, 'Gerente de Compras', 'COMPRAS'),
                (1, 'Jefe de Compras', 'COMPRAS'),
                (1, 'Comprador Senior', 'COMPRAS'),
                (1, 'Comprador Junior', 'COMPRAS'),
                (1, 'Analista de Cotizaciones', 'COMPRAS'),
                (1, 'Responsable de Abastecimiento', 'COMPRAS'),
                (1, 'Gerente Comercial', 'VENTAS'),
                (1, 'Jefe de Ventas', 'VENTAS'),
                (1, 'Ejecutivo de Cuentas', 'VENTAS'),
                (1, 'Vendedor Senior', 'VENTAS'),
                (1, 'Asistente de Ventas', 'VENTAS'),
                (1, 'Responsable de Facturación', 'VENTAS'),
                (1, 'Director General', 'GENERAL'),
                (1, 'Gerente Administrativo', 'GENERAL'),
                (1, 'Jefe de Tesorería', 'GENERAL'),
                (1, 'Analista de Cuentas a Pagar', 'GENERAL'),
                (1, 'Analista de Cuentas a Cobrar', 'GENERAL')
            ]
            cursor.executemany("INSERT INTO erp_puestos (enterprise_id, nombre, modulo) VALUES (?, ?, ?)", puestos)
            conn.commit()
            logger.info(f"Se insertaron {len(puestos)} puestos.")
        else:
            logger.info(f"La tabla erp_puestos ya contiene {count} registros.")

        conn.close()
        print("Migración de puestos completada exitosamente.")

    except Exception as e:
        logger.error(f"Error en migración: {e}")
        exit(1)

if __name__ == "__main__":
    migrate()
