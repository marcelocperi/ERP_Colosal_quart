
import sys
import os
sys.path.append(os.path.dirname(__file__))
from database import get_db_cursor

def check_tables():
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'cmp_sourcing_origenes'")
            row = cursor.fetchone()
            if row:
                print("Table 'cmp_sourcing_origenes' exists.")
            else:
                print("Table 'cmp_sourcing_origenes' NOT FOUND.")
                
            cursor.execute("SHOW TABLES LIKE 'cmp_articulos_proveedores'")
            row = cursor.fetchone()
            if row:
                print("Table 'cmp_articulos_proveedores' exists.")
            else:
                print("Table 'cmp_articulos_proveedores' NOT FOUND.")
    except Exception as e:
        print(f"Error checking DB: {e}")

if __name__ == "__main__":
    check_tables()
