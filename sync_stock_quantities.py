
import mariadb
from database import DB_CONFIG

def sync_stock():
    print("Iniciando sincronización de stock con numero_ejemplares...")
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # 1. Obtener todos los libros con sus cantidades
        cursor.execute("SELECT id, enterprise_id, IFNULL(numero_ejemplares, 1) as q FROM libros")
        libros = cursor.fetchall()
        print(f"Procesando {len(libros)} libros...")
        
        for lib in libros:
            lib_id = lib['id']
            ent_id = lib['enterprise_id']
            qty = lib['q']
            
            # 2. Buscar/Crear el depósito principal para esta empresa
            cursor.execute("SELECT id FROM stk_depositos WHERE enterprise_id = %s AND es_principal = 1 LIMIT 1", (ent_id,))
            dep = cursor.fetchone()
            
            if not dep:
                # Si no hay depósito principal, buscar el primero que haya
                cursor.execute("SELECT id FROM stk_depositos WHERE enterprise_id = %s LIMIT 1", (ent_id,))
                dep = cursor.fetchone()
            
            if not dep:
                # Crear uno por defecto si no existe ninguno
                print(f"  Creando depósito principal para Empresa {ent_id}...")
                cursor.execute("INSERT INTO stk_depositos (enterprise_id, nombre, es_principal) VALUES (%s, 'Depósito Central', 1)", (ent_id,))
                cursor.execute("SELECT LAST_INSERT_ID() as id")
                dep_id = cursor.fetchone()['id']
            else:
                dep_id = dep['id']
                
            # 3. Sincronizar existencia
            # Usamos INSERT ON DUPLICATE KEY UPDATE para MariaDB
            cursor.execute("""
                INSERT INTO stk_existencias (enterprise_id, deposito_id, articulo_id, cantidad, last_updated)
                VALUES (%s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE 
                    cantidad = VALUES(cantidad),
                    last_updated = NOW()
            """, (ent_id, dep_id, lib_id, qty))
            
        conn.commit()
        print("Sincronización finalizada exitosamente.")
        conn.close()
    except Exception as e:
        print(f"Error durante la sincronización: {e}")

if __name__ == "__main__":
    sync_stock()
