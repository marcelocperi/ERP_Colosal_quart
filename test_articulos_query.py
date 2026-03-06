import sys
sys.path.insert(0, 'c:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP')

from database import get_db_cursor

# Simular el query que usa la ruta /articulos
with get_db_cursor() as cursor:
    cursor.execute("""
        SELECT id, nombre, modelo as autor, JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.genero')) as genero, 
               codigo as isbn, precio_venta as precio, 
               JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.fecha_publicacion')) as fecha_publicacion, 
               marca as editorial, 
               JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.descripcion')) as descripcion, 
               IFNULL((SELECT SUM(cantidad) FROM stk_existencias WHERE articulo_id = stk_articulos.id AND enterprise_id = stk_articulos.enterprise_id), 0) as numero_ejemplares,
               IFNULL((SELECT COUNT(*) FROM prestamos WHERE libro_id = stk_articulos.id AND fecha_devolucion_real IS NULL AND enterprise_id = stk_articulos.enterprise_id), 0) as prestados,
               IFNULL((SELECT SUM(cantidad) FROM movimientos_pendientes WHERE libro_id = stk_articulos.id AND tipo = 'compra' AND estado = 'pendiente' AND enterprise_id = stk_articulos.enterprise_id), 0) as en_camino,
               IFNULL((SELECT SUM(cantidad) FROM movimientos_pendientes WHERE libro_id = stk_articulos.id AND (tipo = 'egreso' OR tipo = 'baja') AND estado = 'pendiente' AND enterprise_id = stk_articulos.enterprise_id), 0) as pendientes,
               JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.paginas')) as numero_paginas,
               JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.cover_url')) as cover_url,
               lengua, origen, tipo_articulo_id
        FROM stk_articulos 
        WHERE enterprise_id = 1
        ORDER BY nombre ASC
        LIMIT 1
    """)
    row = cursor.fetchone()
    
    if row:
        print(f"✓ Query ejecutada correctamente")
        print(f"Total columnas: {len(row)}")
        print(f"\nÍndices esperados por el template:")
        print(f"  [0] id: {row[0]}")
        print(f"  [1] nombre: {row[1]}")
        print(f"  [2] autor: {row[2]}")
        print(f"  [9] stock_fisico: {row[9]}")
        print(f"  [10] prestados: {row[10]}")
        print(f"  [14] cover_url: {row[14]}")
        print(f"  [15] lengua: {row[15]}")
        print(f"  [16] origen: {row[16]}")
        print(f"  [17] tipo_articulo_id: {row[17]}")
    else:
        print("✗ No hay artículos en la base de datos")
