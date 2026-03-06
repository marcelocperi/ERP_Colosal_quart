
import mariadb
from database import DB_CONFIG

def fix_duplicates_migration():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("Fixing Duplicates and Migrating from Ent 4 to Ent 1...")
    
    # Get all books in Ent 4
    cursor.execute("SELECT id, isbn, nombre FROM libros WHERE enterprise_id = 4")
    books_ent4 = cursor.fetchall()
    
    updated_count = 0
    merged_count = 0
    
    for b4_id, b4_isbn, b4_title in books_ent4:
        # Check if ISBN exists in Ent 1
        cursor.execute("SELECT id FROM libros WHERE isbn = ? AND enterprise_id = 1", (b4_isbn,))
        row_ent1 = cursor.fetchone()
        
        if row_ent1:
            # Duplicate found! Merge references to Ent 1 book
            b1_id = row_ent1[0]
            # print(f"Merging Book {b4_id} ({b4_isbn}) into {b1_id}...")
            
            try:
                # Update Loans
                cursor.execute("UPDATE prestamos SET libro_id = ? WHERE libro_id = ?", (b1_id, b4_id))
                
                # Update Movimientos
                cursor.execute("UPDATE movimientos_pendientes SET libro_id = ? WHERE libro_id = ?", (b1_id, b4_id))
                cursor.execute("UPDATE stock_ajustes SET libro_id = ? WHERE libro_id = ?", (b1_id, b4_id))
                
                # Update Reservas (if dynamic logic used IDs, verified above usually)

                # Delete the old Ent 4 book
                cursor.execute("DELETE FROM libros WHERE id = ?", (b4_id,))
                merged_count += 1
            except Exception as e:
                print(f"Error merging {b4_id}: {e}")
        else:
            # No duplicate, just move it
            try:
                cursor.execute("UPDATE libros SET enterprise_id = 1 WHERE id = ?", (b4_id,))
                updated_count += 1
            except Exception as e:
                # Handle edge case where ISBN is null or something
                print(f"Error moving {b4_id}: {e}")
                
    conn.commit()
    print(f"Migration Complete: {merged_count} merged, {updated_count} moved.")
    conn.close()

if __name__ == "__main__":
    fix_duplicates_migration()
