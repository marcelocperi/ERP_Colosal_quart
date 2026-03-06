
import mariadb
from database import DB_CONFIG

def check_book_6587():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM libros WHERE id = 6587")
    row = cursor.fetchone()
    print(f"Book 6587: {row}")
    
    # Check column names to be sure which one is enterprise_id
    cursor.execute("DESCRIBE libros")
    print(cursor.fetchall())
    
    conn.close()

if __name__ == "__main__":
    check_book_6587()
