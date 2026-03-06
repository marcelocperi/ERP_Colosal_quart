
from database import get_db_cursor

def migrate():
    try:
        with get_db_cursor() as cursor:
            # Check if columns exist
            cursor.execute("DESCRIBE stk_articulos")
            columns = [col[0] for col in cursor.fetchall()]
            
            if 'costo_reposicion' not in columns:
                print("Adding costo_reposicion to stk_articulos...")
                cursor.execute("ALTER TABLE stk_articulos ADD COLUMN costo_reposicion DECIMAL(10,2) DEFAULT 0.00")
            
            if 'fecha_costo_reposicion' not in columns:
                print("Adding fecha_costo_reposicion to stk_articulos...")
                cursor.execute("ALTER TABLE stk_articulos ADD COLUMN fecha_costo_reposicion DATE")
                
            print("Migration completed successfully.")
    except Exception as e:
        print(f"Error during migration: {e}")

if __name__ == "__main__":
    migrate()
