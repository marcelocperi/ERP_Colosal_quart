import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

def run():
    print("🚀 Creating Tax Engine Versioning Tables...")
    with get_db_cursor() as cursor:
        # 1. Table for Version History
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tax_engine_versions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL DEFAULT 0,
                version_code VARCHAR(50) NOT NULL,
                fecha_implementacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                usuario_id INT,
                descripcion TEXT,
                UNIQUE KEY uq_version (enterprise_id, version_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 2. Table for Snapshots (Full backup of rules at that version)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tax_engine_snapshots (
                id INT AUTO_INCREMENT PRIMARY KEY,
                version_id INT NOT NULL,
                reglas_json LONGTEXT,
                alicuotas_json LONGTEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (version_id) REFERENCES tax_engine_versions(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        
        # 3. Add initial version 1.0 if empty
        cursor.execute("SELECT COUNT(*) FROM tax_engine_versions")
        if cursor.fetchone()[0] == 0:
            print("  📌 Initializing Version 1.0...")
            cursor.execute("""
                INSERT INTO tax_engine_versions (enterprise_id, version_code, descripcion, usuario_id)
                VALUES (0, '1.0', 'Versión Inicial del Motor Fiscal', 1)
            """)
            
    print("✅ Tables created successfully.")

if __name__ == "__main__":
    run()
