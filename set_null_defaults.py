
import mariadb
from database import DB_CONFIG

def adjust_nullable():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Cambiando columnas a NULL por defecto...")
        cursor.execute("ALTER TABLE libros MODIFY COLUMN lengua VARCHAR(3) DEFAULT NULL")
        cursor.execute("ALTER TABLE libros MODIFY COLUMN origen VARCHAR(20) DEFAULT NULL")
        
        print("Limpiando UNDs existentes para re-procesar...")
        cursor.execute("UPDATE libros SET lengua = NULL, origen = NULL WHERE lengua = 'UND' OR lengua = ''")
        
        conn.commit()
        conn.close()
        print("Éxito: Ahora el criterio de entrada es NULL.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    adjust_nullable()
