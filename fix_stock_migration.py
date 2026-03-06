from database import get_db_cursor
import json

def migrate_stock_quantities():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- INICIANDO MIGRACIÓN DE CANTIDADES DE STOCK ---")
        
        # 1. Verificar si existe la tabla legacy_libros o libros
        cursor.execute("SHOW TABLES LIKE 'legacy_libros'")
        has_legacy = cursor.fetchone()
        table_name = 'legacy_libros' if has_legacy else 'libros'
        
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if not cursor.fetchone():
            print(f"Error: No se encontró la tabla '{table_name}'.")
            return

        # 2. Asegurar que todas las empresas tengan un depósito central
        cursor.execute("SELECT DISTINCT enterprise_id FROM stk_articulos")
        enterprises = [row['enterprise_id'] for row in cursor.fetchall()]
        
        for ent_id in enterprises:
            cursor.execute("SELECT id FROM stk_depositos WHERE enterprise_id = %s AND es_principal = 1", (ent_id,))
            if not cursor.fetchone():
                print(f"Creando depósito principal para empresa {ent_id}...")
                cursor.execute("""
                    INSERT INTO stk_depositos (enterprise_id, nombre, codigo, es_principal, activo)
                    VALUES (%s, 'Depósito Central', 'CENTRAL', 1, 1)
                """, (ent_id,))
        
        # 3. Migrar cantidades
        print(f"Migrando cantidades desde {table_name}...")
        cursor.execute(f"SELECT id, enterprise_id, isbn, numero_ejemplares FROM {table_name}")
        old_books = cursor.fetchall()
        
        migrated = 0
        for b in old_books:
            qty = b['numero_ejemplares']
            if qty <= 0: continue
            
            # Buscar el nuevo ID del artículo en stk_articulos usando el ISBN y enterprise_id
            cursor.execute("SELECT id FROM stk_articulos WHERE codigo = %s AND enterprise_id = %s", (b['isbn'], b['enterprise_id']))
            new_art = cursor.fetchone()
            
            if not new_art:
                print(f"Aviso: No se encontró artículo para ISBN {b['isbn']} en empresa {b['enterprise_id']}")
                continue
            
            # Buscar el depósito principal de la empresa
            cursor.execute("SELECT id FROM stk_depositos WHERE enterprise_id = %s AND es_principal = 1", (b['enterprise_id'],))
            dep = cursor.fetchone()
            
            if dep:
                # Insertar o actualizar existencia
                cursor.execute("""
                    INSERT INTO stk_existencias (enterprise_id, deposito_id, articulo_id, cantidad)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE cantidad = %s
                """, (b['enterprise_id'], dep['id'], new_art['id'], qty, qty))
                migrated += 1
        
        print(f"Migración de stock completada: {migrated} artículos actualizados.")

migrate_stock_quantities()
