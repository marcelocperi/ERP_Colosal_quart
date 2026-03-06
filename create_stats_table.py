
import mariadb
from database import DB_CONFIG

def create_stats_table():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_stats (
                key_name VARCHAR(50) PRIMARY KEY, 
                value_int INT DEFAULT 0,
                value_str TEXT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        
        # Add column if table already exists without it
        try:
            cursor.execute("ALTER TABLE system_stats ADD COLUMN value_str TEXT NULL")
        except: pass
        
        # Inicializar contadores si no existen
        cursor.execute("INSERT IGNORE INTO system_stats (key_name, value_int, value_str) VALUES ('batch_processed', 0, '')")
        cursor.execute("INSERT IGNORE INTO system_stats (key_name, value_int, value_str) VALUES ('batch_status', 0, 'Inactivo')")
        
        conn.commit()
        conn.close()
        print("Table system_stats ready")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_stats_table()
