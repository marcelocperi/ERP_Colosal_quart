from database import get_db_cursor

def migrate():
    with get_db_cursor() as cursor:
        print("Checking cmp_items_cotizacion for cantidad_ofrecida...")
        cursor.execute("SHOW COLUMNS FROM cmp_items_cotizacion LIKE 'cantidad_ofrecida'")
        if not cursor.fetchone():
            print("Adding column cantidad_ofrecida to cmp_items_cotizacion...")
            cursor.execute("ALTER TABLE cmp_items_cotizacion ADD COLUMN cantidad_ofrecida INT AFTER cantidad")
        
        cursor.execute("SHOW COLUMNS FROM cmp_items_cotizacion LIKE 'precio_cotizado'")
        if not cursor.fetchone():
            print("Adding column precio_cotizado to cmp_items_cotizacion...")
            cursor.execute("ALTER TABLE cmp_items_cotizacion ADD COLUMN precio_cotizado DECIMAL(10,2) AFTER precio_ofrecido")
        else:
            print("Column precio_cotizado already exists.")
            
        # Also check if we need to sync other columns mentioned in mailers
        # purchase_order_mailer.py uses 'precio_cotizado' but schema has 'precio_ofrecido'
        # Let's add 'precio_cotizado' as an alias or rename or just ensure it works.
        # Actually, let's keep 'precio_ofrecido' as per schema but check usage.
        
        cursor.execute("SHOW COLUMNS FROM cmp_ordenes_compra LIKE 'fecha_emision'")
        if not cursor.fetchone():
            print("Adding column fecha_emision to cmp_ordenes_compra...")
            cursor.execute("ALTER TABLE cmp_ordenes_compra ADD COLUMN fecha_emision DATETIME AFTER fecha")
            
        cursor.execute("SHOW COLUMNS FROM cmp_ordenes_compra LIKE 'total_estimado'")
        if not cursor.fetchone():
            print("Adding column total_estimado to cmp_ordenes_compra...")
            cursor.execute("ALTER TABLE cmp_ordenes_compra ADD COLUMN total_estimado DECIMAL(10,2) AFTER total")

    print("Migration finished.")

if __name__ == "__main__":
    migrate()
