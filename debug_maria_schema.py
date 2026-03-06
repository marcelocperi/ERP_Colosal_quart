import mariadb
from database import DB_CONFIG

def check_libros_schema():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        print("--- Estructura de tabla LIBROS ---")
        cursor.execute("DESCRIBE libros")
        for row in cursor.fetchall():
            print(f"Field: {row['Field']}, Type: {row['Type']}, Null: {row['Null']}")
            
        print("\n--- Estructura de tabla STK_MOTIVOS ---")
        cursor.execute("DESCRIBE stk_motivos")
        for row in cursor.fetchall():
            print(f"Field: {row['Field']}, Type: {row['Type']}, Null: {row['Null']}")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_libros_schema()
