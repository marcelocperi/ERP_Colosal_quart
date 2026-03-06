from database import get_db_cursor
import json

def audit_stock():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- AUDITORÍA DE STOCK ---")
        
        # 1. Cantidad de artículos
        cursor.execute("SELECT COUNT(*) as cant FROM stk_articulos")
        print(f"Total artículos en stk_articulos: {cursor.fetchone()['cant']}")
        
        # 2. Cantidad de existencias registradas
        cursor.execute("SELECT COUNT(*) as cant FROM stk_existencias")
        print(f"Total registros en stk_existencias: {cursor.fetchone()['cant']}")
        
        # 3. Suma total de unidades
        cursor.execute("SELECT SUM(cantidad) as total FROM stk_existencias")
        print(f"Total unidades físicas en stock: {cursor.fetchone()['total']}")
        
        # 4. Verificar si hay artículos sin existencia inicial
        cursor.execute("""
            SELECT COUNT(*) as cant 
            FROM stk_articulos a 
            LEFT JOIN stk_existencias e ON a.id = e.articulo_id 
            WHERE e.articulo_id IS NULL
        """)
        print(f"Artículos sin registro de existencia: {cursor.fetchone()['cant']}")

        # 5. Muestra de los primeros 5 artículos y su stock calculado
        print("\nTop 5 artículos y su stock:")
        cursor.execute("""
            SELECT l.nombre, l.codigo as isbn,
                   (SELECT IFNULL(SUM(cantidad), 0) FROM stk_existencias WHERE articulo_id = l.id) as stock_total
            FROM stk_articulos l
            LIMIT 5
        """)
        for row in cursor.fetchall():
            print(f"- {row['nombre']} ({row['isbn']}): Stock {row['stock_total']}")

audit_stock()
