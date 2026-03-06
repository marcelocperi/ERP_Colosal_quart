import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from multiMCP.database import get_db_cursor

def setup_series_audit():
    print("Fase 1.3b — Creando Motor de Trazabilidad Cíclica (Log de Vida)...")
    with get_db_cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_series_trazabilidad (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                serie_id INT NOT NULL,
                tipo_evento ENUM('INGRESO','VENTA','DEVOLUCION','AJUSTE','BAJA') NOT NULL,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tercero_id INT DEFAULT NULL COMMENT 'Cliente o Proveedor',
                comprobante_id INT DEFAULT NULL COMMENT 'Factura, NC, Remito, etc.',
                user_id INT NOT NULL,
                estado_resultante VARCHAR(20),
                notas TEXT,
                INDEX idx_serie (serie_id),
                INDEX idx_fecha (fecha)
            )
        """)
        print("  Tabla stk_series_trazabilidad creada/verificada.")

    print("Setup de auditoría completado.")

if __name__ == "__main__":
    setup_series_audit()
