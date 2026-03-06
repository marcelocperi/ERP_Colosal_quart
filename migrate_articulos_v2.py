from database import get_db_cursor
import datetime

def migrate():
    try:
        with get_db_cursor() as cursor:
            # Check if column exists
            cursor.execute("DESCRIBE stk_articulos")
            columns = [col[0] for col in cursor.fetchall()]
            
            if 'metodo_costeo' not in columns:
                print("Adding metodo_costeo to stk_articulos...")
                cursor.execute("ALTER TABLE stk_articulos ADD COLUMN metodo_costeo VARCHAR(50) DEFAULT 'CPP'")
            
            # Perform mass update
            print("Performing mass update of costs and costing method...")
            today = datetime.date.today().strftime('%Y-%m-%d')
            
            # Update costo_reposicion to 0.85 * precio_venta and set metodo_costeo to 'CPP'
            # Note: We use COALESCE(precio_venta, 0) to avoid NULL issues
            cursor.execute("""
                UPDATE stk_articulos 
                SET metodo_costeo = 'CPP',
                    costo_reposicion = COALESCE(precio_venta, 0) * 0.85,
                    fecha_costo_reposicion = %s
            """, (today,))
            
            print(f"Migration and update completed successfully for all enterprises.")
    except Exception as e:
        print(f"Error during migration: {e}")

if __name__ == "__main__":
    migrate()
