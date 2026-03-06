from database import get_db_cursor

def fix_loans_and_create_view():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- REPARANDO RELACIONES DE PRÉSTAMOS ---")
        
        # 1. Actualizar registrar en la tabla prestamos
        # Mapear los IDs antiguos a los nuevos basados en el ISBN (codigo)
        cursor.execute("""
            UPDATE prestamos p
            JOIN legacy_libros l ON p.libro_id = l.id
            JOIN stk_articulos a ON l.isbn = a.codigo AND l.enterprise_id = a.enterprise_id
            SET p.libro_id = a.id
        """)
        print(f"Préstamos actualizados: {cursor.rowcount}")

        # 2. Crear VISTA 'libros' para compatibilidad con código antiguo
        print("Creando vista 'libros' para compatibilidad...")
        cursor.execute("DROP VIEW IF EXISTS libros")
        cursor.execute("""
            CREATE VIEW libros AS
            SELECT 
                a.id,
                a.enterprise_id,
                a.nombre,
                a.modelo AS autor,
                a.marca AS editorial,
                a.codigo AS isbn,
                (SELECT IFNULL(SUM(cantidad), 0) FROM stk_existencias WHERE articulo_id = a.id) AS numero_ejemplares,
                a.precio_venta AS precio,
                a.lengua,
                a.origen,
                a.api_checked,
                a.created_at,
                JSON_UNQUOTE(JSON_EXTRACT(a.metadata_json, '$.genero')) AS genero,
                JSON_UNQUOTE(JSON_EXTRACT(a.metadata_json, '$.fecha_publicacion')) AS fecha_publicacion,
                JSON_UNQUOTE(JSON_EXTRACT(a.metadata_json, '$.paginas')) AS numero_paginas
            FROM stk_articulos a
        """)
        print("Vista 'libros' creada exitosamente.")

fix_loans_and_create_view()
