
from database import get_db_cursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('migration')

def migrate():
    try:
        with get_db_cursor() as cursor:
            # 1. Crear tabla de detalles de comprobantes
            logger.info("Creando tabla erp_comprobantes_detalle...")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS erp_comprobantes_detalle (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    comprobante_id INT NOT NULL,
                    articulo_id INT,
                    descripcion VARCHAR(255) NOT NULL,
                    cantidad DECIMAL(15, 3) NOT NULL,
                    precio_unitario DECIMAL(15, 2) NOT NULL,
                    alicuota_iva DECIMAL(5, 2) DEFAULT 21.00,
                    subtotal_neto DECIMAL(15, 2) NOT NULL,
                    importe_iva DECIMAL(15, 2) NOT NULL,
                    subtotal_total DECIMAL(15, 2) NOT NULL,
                    FOREIGN KEY (comprobante_id) REFERENCES erp_comprobantes(id) ON DELETE CASCADE
                )
            ''')
            
            # 2. Crear tabla de tipos de comprobantes AFIP
            logger.info("Creando tabla sys_tipos_comprobante...")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sys_tipos_comprobante (
                    codigo VARCHAR(5) PRIMARY KEY,
                    descripcion VARCHAR(100) NOT NULL,
                    letra VARCHAR(1) NOT NULL
                )
            ''')
            
            # 3. Seed data AFIP (Facturas A, B, C)
            logger.info("Insertando tipos de comprobante AFIP...")
            tipos = [
                ('001', 'Factura A', 'A'),
                ('006', 'Factura B', 'B'),
                ('011', 'Factura C', 'C'),
                ('003', 'Nota de Crédito A', 'A'),
                ('008', 'Nota de Crédito B', 'B')
            ]
            for t in tipos:
                cursor.execute("INSERT IGNORE INTO sys_tipos_comprobante (codigo, descripcion, letra) VALUES (%s, %s, %s)", t)
            
            print('✅ Schema updated successfully.')
    except Exception as e:
        logger.error(f"Error en la migración: {e}")

if __name__ == "__main__":
    migrate()
