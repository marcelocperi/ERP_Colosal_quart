
from database import get_db_cursor

def migrate():
    try:
        with get_db_cursor() as cursor:
            print("Añadiendo enterprise_id a tablas de detalles de terceros...")
            
            tables = ['erp_direcciones', 'erp_contactos', 'erp_datos_fiscales']
            
            for table in tables:
                try:
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN enterprise_id INT AFTER id")
                    print(f"Columna enterprise_id añadida a {table}")
                except Exception as err:
                    err_str = str(err)
                    if "Duplicate column name" in err_str or "1060" in err_str:
                        print(f"Columna enterprise_id ya existe en {table}")
                    else:
                        print(f"Error en {table}: {err}")
            
            # Sincronizar enterprise_id desde el tercero padre
            print("Sincronizando enterprise_id desde erp_terceros...")
            cursor.execute("UPDATE erp_direcciones d JOIN erp_terceros t ON d.tercero_id = t.id SET d.enterprise_id = t.enterprise_id WHERE d.enterprise_id IS NULL")
            cursor.execute("UPDATE erp_contactos c JOIN erp_terceros t ON c.tercero_id = t.id SET c.enterprise_id = t.enterprise_id WHERE c.enterprise_id IS NULL")
            cursor.execute("UPDATE erp_datos_fiscales f JOIN erp_terceros t ON f.tercero_id = t.id SET f.enterprise_id = t.enterprise_id WHERE f.enterprise_id IS NULL")
            
            print("Migración completada exitosamente.")
        
    except Exception as e:
        print(f"Error general: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    migrate()
