
import mariadb
from database import DB_CONFIG

def check_and_fix_stock():
    print("Analizando duplicados y stock entre Empresa 4 y Empresa 1...")
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # 1. Contar stock en Ent 4
        cursor.execute("SELECT SUM(cantidad) as total FROM stk_existencias WHERE enterprise_id = 4")
        total_4 = cursor.fetchone()['total'] or 0
        print(f"Stock total en Empresa 4: {total_4}")
        
        # 2. Si el usuario dice que el stock pertenece a la 1, vamos a migrarlo/sumarlo
        if total_4 > 0:
            print("Migrando stock de Empresa 4 a Empresa 1...")
            
            # Buscamos el depósito principal de la 1
            cursor.execute("SELECT id FROM stk_depositos WHERE enterprise_id = 1 AND es_principal = 1 LIMIT 1")
            dep_1 = cursor.fetchone()
            if not dep_1:
                cursor.execute("SELECT id FROM stk_depositos WHERE enterprise_id = 1 LIMIT 1")
                dep_1 = cursor.fetchone()
            
            if not dep_1:
                print("Error: No se encontró depósito en Empresa 1")
                return
                
            dep_1_id = dep_1['id']
            
            # Obtenemos existencias de la 4
            cursor.execute("SELECT articulo_id, cantidad FROM stk_existencias WHERE enterprise_id = 4")
            existencias_4 = cursor.fetchall()
            
            for ex in existencias_4:
                art_id = ex['articulo_id']
                qty = ex['cantidad']
                
                # Para cada artículo en la 4, verificamos si existe en la 1 (por ID o por ISBN)
                # Dado que stk_existencias.articulo_id referencia libros.id, y los IDs son únicos por tabla,
                # si los libros fueron migrados de 4 a 1, el ID en la 1 podría ser distinto.
                
                # Buscamos el libro correspondiente en la 1 usando el ISBN del libro de la 4
                cursor.execute("SELECT isbn FROM libros WHERE id = %s AND enterprise_id = 4", (art_id,))
                lib4 = cursor.fetchone()
                if not lib4: continue
                isbn = lib4['isbn']
                
                cursor.execute("SELECT id FROM libros WHERE isbn = %s AND enterprise_id = 1", (isbn,))
                lib1 = cursor.fetchone()
                
                if lib1:
                    target_art_id = lib1['id']
                    # Actualizar o insertar existencia en la 1
                    cursor.execute("""
                        INSERT INTO stk_existencias (enterprise_id, deposito_id, articulo_id, cantidad, last_updated)
                        VALUES (1, %s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE 
                            cantidad = cantidad + VALUES(cantidad),
                            last_updated = NOW()
                    """, (dep_1_id, target_art_id, qty))
                else:
                    print(f"Aviso: Libro con ISBN {isbn} no encontrado en Empresa 1. No se migró su stock.")

            # 3. Limpiar stock de la empresa 4
            cursor.execute("DELETE FROM stk_existencias WHERE enterprise_id = 4")
            print("Stock de Empresa 4 eliminado.")
            
            # 4. También eliminar libros de la empresa 4 si son duplicados
            cursor.execute("DELETE FROM libros WHERE enterprise_id = 4")
            print("Libros de Empresa 4 eliminados para evitar confusión.")

        conn.commit()
        print("Operación completada.")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_and_fix_stock()
