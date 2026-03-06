
import mariadb
from database import DB_CONFIG

def fix_defaults():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Cambiando defaults a 'UND'...")
        cursor.execute("ALTER TABLE libros MODIFY COLUMN lengua VARCHAR(3) DEFAULT 'UND'")
        cursor.execute("ALTER TABLE libros MODIFY COLUMN origen VARCHAR(20) DEFAULT 'UND'")
        
        print("Actualizando registros existentes...")
        cursor.execute("UPDATE libros SET lengua = 'UND' WHERE lengua IS NULL OR lengua = '' OR lengua = 'spa'")
        cursor.execute("UPDATE libros SET origen = 'UND' WHERE origen IS NULL OR origen = '' OR origen = 'Local'")
        
        conn.commit()
        print("Éxito: Todos los registros sin información ahora son 'UND'.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fix_defaults()
