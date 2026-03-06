import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from multiMCP.database import get_db_cursor

def setup_serials():
    print("Configurando tabla stk_series para Serial Number Engine...")
    with get_db_cursor() as cursor:

        # Ensure stk_series exists (may already be there from previous phases)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_series (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                articulo_id INT NOT NULL,
                numero_serie VARCHAR(100) NOT NULL,
                origen ENUM('MANUAL_SCAN','IMPORTACION','AUTOGENERADO') DEFAULT 'MANUAL_SCAN',
                estado ENUM('EN_STOCK','VENDIDO','RESERVADO','DADO_BAJA') DEFAULT 'EN_STOCK',
                recepcion_id INT DEFAULT NULL,
                lote VARCHAR(50) DEFAULT NULL,
                notas VARCHAR(255) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_serie_empresa (enterprise_id, numero_serie),
                INDEX idx_art (articulo_id)
            )
        """)
        print("Tabla stk_series OK.")

        # Track correlative counter per article (for auto-generation)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_series_counter (
                enterprise_id INT NOT NULL,
                articulo_id INT NOT NULL,
                ultimo_correlativo INT DEFAULT 0,
                prefijo VARCHAR(20) DEFAULT NULL,
                PRIMARY KEY (enterprise_id, articulo_id)
            )
        """)
        print("Tabla stk_series_counter OK.")

    print("Serial Number Engine setup completado.")

if __name__ == "__main__":
    setup_serials()
