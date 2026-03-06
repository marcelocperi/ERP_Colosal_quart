
import mariadb
from database import DB_CONFIG

def update_services_config():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Modifying sys_external_services table...")
        
        # 1. Add modo_captura
        cursor.execute("SHOW COLUMNS FROM sys_external_services LIKE 'modo_captura'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE sys_external_services ADD COLUMN modo_captura VARCHAR(20) DEFAULT 'API'")
            print("Added column: modo_captura")

        # 2. Add url_objetivo
        cursor.execute("SHOW COLUMNS FROM sys_external_services LIKE 'url_objetivo'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE sys_external_services ADD COLUMN url_objetivo VARCHAR(255)")
            print("Added column: url_objetivo")
            
        # 3. Add system_code to identify standard services easily
        cursor.execute("SHOW COLUMNS FROM sys_external_services LIKE 'system_code'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE sys_external_services ADD COLUMN system_code VARCHAR(50)")
            print("Added column: system_code")

        # --- SEEDING DATA ---
        
        # Update OpenLibrary
        cursor.execute("""
            UPDATE sys_external_services 
            SET modo_captura = 'API', url_objetivo = 'https://openlibrary.org', system_code = 'OPEN_LIBRARY'
            WHERE nombre = 'OpenLibraryService' AND enterprise_id = 1
        """)
        
        # Insert Native Service explicitly
        cursor.execute("SELECT id FROM sys_external_services WHERE system_code = 'NATIVE' AND enterprise_id = 1")
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO sys_external_services (enterprise_id, nombre, tipo_servicio, clase_implementacion, config_json, modo_captura, url_objetivo, system_code)
                VALUES (1, 'Carga Manual (Nativo)', 'DATA_PROVIDER', 'services.book_service_factory.NativeService', '{}', 'NATIVE', NULL, 'NATIVE')
            """)
            print("Inserted: Native Service")
            
        # Insert Example Scraping Service (Placeholder) for Goodreads
        cursor.execute("SELECT id FROM sys_external_services WHERE system_code = 'GOODREADS_SCRAPE' AND enterprise_id = 1")
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO sys_external_services (enterprise_id, nombre, tipo_servicio, clase_implementacion, config_json, modo_captura, url_objetivo, system_code)
                VALUES (1, 'Goodreads Scraper', 'DATA_PROVIDER', 'services.scraping_service.GoodreadsScraper', '{"wait_time": 2}', 'SCRAPING', 'https://www.goodreads.com/search?q={isbn}', 'GOODREADS_SCRAPE')
            """)
            print("Inserted: Goodreads Scraping Service Example")

        conn.commit()
        conn.close()
        print("Table structure and seeds updated.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_services_config()
