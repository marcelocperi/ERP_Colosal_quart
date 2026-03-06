
import mariadb
from database import DB_CONFIG

def force_migrate():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Check count of Ent 4 in Libros
    cursor.execute("SELECT COUNT(*) FROM libros WHERE enterprise_id = 4")
    cnt = cursor.fetchone()[0]
    print(f"Books in Ent 4: {cnt}")
    
    if cnt > 0:
        cursor.execute("UPDATE libros SET enterprise_id = 1 WHERE enterprise_id = 4")
        print(f"Updated {cursor.rowcount} books from Ent 4 to 1")
    else:
        print("No books in Ent 4 to update.")
        
    # Check if there are other enterprises
    cursor.execute("SELECT enterprise_id, COUNT(*) FROM libros GROUP BY enterprise_id")
    print("Books per Enterprise:")
    for r in cursor.fetchall():
        print(r)
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    force_migrate()
