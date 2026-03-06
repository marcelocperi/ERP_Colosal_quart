
import mariadb
from database import DB_CONFIG

def create_services_table():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Creating sys_external_services table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sys_external_services (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                nombre VARCHAR(100) NOT NULL, -- e.g. OpenLibrary
                tipo_servicio VARCHAR(50) DEFAULT 'DATA_PROVIDER', -- DATA_PROVIDER, PAYMENT_GATEWAY, etc
                clase_implementacion VARCHAR(200), -- Python class path for dynamic loading
                config_json JSON, -- configuration like API keys
                activo BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print("Creating relation table stk_tipos_articulo_servicios...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stk_tipos_articulo_servicios (
                id INT AUTO_INCREMENT PRIMARY KEY,
                enterprise_id INT NOT NULL,
                tipo_articulo_id INT NOT NULL,
                servicio_id INT, -- NULL means NATIVE
                config_overwrite_json JSON, -- Logic specific config for this type
                es_primario BOOLEAN DEFAULT 1,
                FOREIGN KEY (tipo_articulo_id) REFERENCES stk_tipos_articulo(id) ON DELETE CASCADE,
                FOREIGN KEY (servicio_id) REFERENCES sys_external_services(id) ON DELETE SET NULL,
                UNIQUE(enterprise_id, tipo_articulo_id, servicio_id)
            )
        """)
        
        # Seed default service
        print("Seeding default services...")
        cursor.execute("SELECT id FROM sys_external_services WHERE nombre = 'OpenLibraryService' AND enterprise_id = 1")
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO sys_external_services (enterprise_id, nombre, tipo_servicio, clase_implementacion, config_json)
                VALUES (1, 'OpenLibraryService', 'DATA_PROVIDER', 'services.library_api_service.LibraryApiService', '{"base_url": "https://openlibrary.org"}')
            """)
            
        # Link Books to OpenLibrary
        cursor.execute("SELECT id FROM stk_tipos_articulo WHERE nombre = 'Libros' AND enterprise_id = 1")
        tipo_row = cursor.fetchone()
        
        cursor.execute("SELECT id FROM sys_external_services WHERE nombre = 'OpenLibraryService' AND enterprise_id = 1")
        serv_row = cursor.fetchone()
        
        if tipo_row and serv_row:
             cursor.execute("""
                INSERT IGNORE INTO stk_tipos_articulo_servicios (enterprise_id, tipo_articulo_id, servicio_id)
                VALUES (1, %s, %s)
             """, (tipo_row[0], serv_row[0]))
             print("Linked Libros to OpenLibraryService")

        conn.commit()
        conn.close()
        print("Services setup completed.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_services_table()
