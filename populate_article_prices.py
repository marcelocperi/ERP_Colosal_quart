
import random
from database import get_db_cursor

def populate_missing_prices():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Seleccionar artículos con precio_venta o costo <= 0 o NULL
            cursor.execute("""
                SELECT id, nombre, precio_venta, costo 
                FROM stk_articulos 
                WHERE precio_venta IS NULL OR precio_venta <= 0 
                   OR costo IS NULL OR costo <= 0
            """)
            articulos = cursor.fetchall()
            
            if not articulos:
                print("No se encontraron artículos sin precio o costo.")
                return

            print(f"Procesando {len(articulos)} artículos...")
            
            # Rangos de precios realistas
            updates = []
            for art in articulos:
                # Generar un costo base aleatorio (entre 1000 y 50000)
                costo = round(random.uniform(1000, 50000), 2)
                # Generar un precio de venta con un margen del 30% al 70%
                margen = random.uniform(1.3, 1.7)
                precio_venta = round(costo * margen, 2)
                
                updates.append((costo, precio_venta, art['id']))
            
            if updates:
                cursor.executemany("""
                    UPDATE stk_articulos 
                    SET costo = %s, precio_venta = %s 
                    WHERE id = %s
                """, updates)
                print(f"✅ Se han actualizado {len(updates)} artículos con precios y costos aleatorios.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    populate_missing_prices()
