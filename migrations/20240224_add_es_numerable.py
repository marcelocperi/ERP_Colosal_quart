import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def migrate():
    with get_db_cursor() as cursor:
        print("Agregando columna es_numerable a erp_comprobantes...")
        try:
            cursor.execute("ALTER TABLE erp_comprobantes ADD COLUMN es_numerable TINYINT(1) DEFAULT 1")
            print("Columna agregada correctamente.")
        except Exception as e:
            if "Duplicate column name" in str(e):
                print("La columna ya existe.")
            else:
                print(f"Error: {e}")

if __name__ == '__main__':
    migrate()
