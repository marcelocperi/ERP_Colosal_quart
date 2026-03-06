
import mariadb
from database import DB_CONFIG

def check_tables():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES LIKE '%articulos%'")
        tables = cursor.fetchall()
        print("Tables found matching '%articulos%':")
        for t in tables:
            print(t[0])
            
        cursor.execute("SHOW TABLES LIKE 'libros'")
        libros = cursor.fetchall()
        if libros:
            print("Table 'libros' exists.")
        else:
            print("Table 'libros' DOES NOT exist.")
            
        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    check_tables()
