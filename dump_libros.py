import mariadb
from database import DB_CONFIG

def dump_libros_columns():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("DESCRIBE libros")
        cols = cursor.fetchall()
        print("COLUMNS IN LIBROS:")
        for col in cols:
            print(f"- {col['Field']} ({col['Type']})")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    dump_libros_columns()
