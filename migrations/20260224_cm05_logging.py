import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def migrate():
    with get_db_cursor() as cursor:
        try:
            cursor.execute("""
                ALTER TABLE erp_terceros_cm05 
                ADD COLUMN user_insert INT NULL,
                ADD COLUMN date_insert DATETIME NULL,
                ADD COLUMN user_update INT NULL,
                ADD COLUMN date_update DATETIME NULL
            """)
        except Exception as e:
            print("Alter table error:", e)
            
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS log_erp_terceros_cm05 (
                    log_id INT AUTO_INCREMENT PRIMARY KEY,
                    id_action INT,
                    fecha_efectiva DATETIME,
                    user_action INT,
                    tercero_id INT,
                    es_cliente TINYINT(1),
                    es_proveedor TINYINT(1),
                    jurisdiccion_code VARCHAR(10),
                    periodo_anio INT,
                    coeficiente DECIMAL(10,4),
                    RECORD_JSON TEXT
                )
            """)
        except Exception as e:
            print("Create log table error:", e)

    print("Migration complete!")

if __name__ == '__main__':
    migrate()
