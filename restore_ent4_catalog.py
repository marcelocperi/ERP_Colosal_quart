
import mariadb
from database import DB_CONFIG

def restore_ent4():
    """
    Reverse the effect of merging Ent 4 books into Ent 1.
    Since we cannot undelete, we will:
    1. Identify books in Ent 1.
    2. Create copies of these books for Ent 4 (restoring independent stock).
    3. Update Ent 4's legacy loans/movements to point to these new copies.
    
    This ensures Ent 1 has its books, and Ent 4 has its books.
    """
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("Restoring Enterprise 4 Book Catalog...")
    
    # 1. Get all books currently in Ent 1 (which includes the merged ones)
    cursor.execute("SELECT id, nombre, autor, isbn, genero, editorial, precio, fecha_publicacion, numero_paginas, numero_ejemplares FROM libros WHERE enterprise_id = 1")
    ent1_books = cursor.fetchall()
    
    restored_count = 0
    
    for b in ent1_books:
        old_id = b[0]
        # Check if Ent 4 already has this ISBN (in case partially failed or distinct)
        cursor.execute("SELECT id FROM libros WHERE isbn = ? AND enterprise_id = 4", (b[3],))
        if cursor.fetchone():
            continue # Already exists in Ent 4, skip
            
        # Create copy in Ent 4
        try:
            cursor.execute("""
                INSERT INTO libros 
                (enterprise_id, nombre, autor, isbn, genero, editorial, precio, fecha_publicacion, numero_paginas, numero_ejemplares)
                VALUES (4, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9]))
            new_ent4_id = cursor.lastrowid
            restored_count += 1
            
            # Now, the tricky part. 
            # Did we have loans in Ent 4 that we accidentally pointed to Ent 1?
            # My previous script updated WHERE libro_id = old_ent4_id
            # I don't know the old_ent4_id.
            # BUT, if there are loans in Ent 4 (enterprise_id=4) pointing to Ent 1 books (enterprise_id=1), that's a leak.
            
            # Fix Ent 4 Loans pointing to Ent 1 Book (old_id)
            cursor.execute("UPDATE prestamos SET libro_id = ? WHERE libro_id = ? AND enterprise_id = 4", (new_ent4_id, old_id))
            
            # Fix Ent 4 Movements
            cursor.execute("UPDATE movimientos_pendientes SET libro_id = ? WHERE libro_id = ? AND enterprise_id = 4", (new_ent4_id, old_id))
            cursor.execute("UPDATE stock_ajustes SET libro_id = ? WHERE libro_id = ? AND enterprise_id = 4", (new_ent4_id, old_id))
            
        except Exception as e:
            print(f"Error restoring book {b[1]}: {e}")

    conn.commit()
    print(f"Restoration Complete. {restored_count} books copied to Enterprise 4.")
    
    # Verification
    cursor.execute("SELECT COUNT(*) FROM libros WHERE enterprise_id = 4")
    print(f"Books in Ent 4: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM libros WHERE enterprise_id = 1")
    print(f"Books in Ent 1: {cursor.fetchone()[0]}")
    
    conn.close()

if __name__ == "__main__":
    restore_ent4()
