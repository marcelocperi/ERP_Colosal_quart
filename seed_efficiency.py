import mariadb
from database import DB_CONFIG

def seed_services():
    """Inicializa la tabla service_efficiency con todos los servicios conocidos"""
    
    # Lista de servicios del sistema (debe coincidir con name_map en enrich_books_api.py)
    services = [
        'Mercado Libre',
        'Librario',
        'Google Books',
        'Open Library',
        'WorldCat',
        'Amazon',
        'Cúspide',
        'Reld'
    ]
    
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Inicializando tabla service_efficiency...")
        
        # Crear tabla si no existe
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS service_efficiency (
                service_name VARCHAR(50) PRIMARY KEY,
                hits_count INT DEFAULT 0,
                fields_provided INT DEFAULT 0,
                ebooks_provided INT DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        
        # Limpiar tabla (opcional - comentar si quieres preservar datos existentes)
        cursor.execute("DELETE FROM service_efficiency")
        
        # Insertar todos los servicios
        for service in services:
            cursor.execute("""
                INSERT INTO service_efficiency (service_name, hits_count, fields_provided, ebooks_provided)
                VALUES (%s, 0, 0, 0)
                ON DUPLICATE KEY UPDATE service_name = service_name
            """, (service,))
            print(f"  ✓ {service}")
        
        conn.commit()
        print(f"\n✓ {len(services)} servicios inicializados correctamente")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    seed_services()
