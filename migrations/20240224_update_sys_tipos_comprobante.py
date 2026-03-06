import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def migrate():
    with get_db_cursor() as cursor:
        print("Actualizando tabla sys_tipos_comprobante...")
        
        columns_to_add = [
            ('es_fiscal', 'TINYINT(1) DEFAULT 0'),
            ('afip_code', 'VARCHAR(5) DEFAULT NULL'),
            ('es_numerable', 'TINYINT(1) DEFAULT 1')
        ]
        
        for col_name, col_type in columns_to_add:
            try:
                cursor.execute(f"ALTER TABLE sys_tipos_comprobante ADD COLUMN {col_name} {col_type}")
                print(f"   [OK] Columna '{col_name}' agregada.")
            except Exception as e:
                if "Duplicate column name" in str(e):
                    print(f"   [SKIP] Columna '{col_name}' ya existe.")
                else:
                    print(f"   [ERROR] Agregando '{col_name}': {e}")

if __name__ == '__main__':
    migrate()
