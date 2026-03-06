
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

def run():
    print("🚀 Creating 'sys_padrones_logs' table...")
    
    with get_db_cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_padrones_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                jurisdiccion VARCHAR(50) NOT NULL,
                fecha_ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP,
                tipo_proceso VARCHAR(20) NOT NULL,
                archivo_origen VARCHAR(255),
                registros_procesados INT DEFAULT 0,
                registros_altas INT DEFAULT 0,
                registros_bajas INT DEFAULT 0, 
                registros_modificaciones INT DEFAULT 0,
                status VARCHAR(20) DEFAULT 'SUCCESS',
                mensaje TEXT,
                INDEX (jurisdiccion),
                INDEX (fecha_ejecucion)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        print("✅ Table 'sys_padrones_logs' created/verified.")

if __name__ == "__main__":
    run()
