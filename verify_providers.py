from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    # Contar proveedores globales
    cursor.execute("""
        SELECT COUNT(*) as total 
        FROM erp_terceros 
        WHERE enterprise_id = 0 AND es_proveedor = 1
    """)
    total = cursor.fetchone()['total']
    
    print(f"Total proveedores globales: {total}")
    
    # Mostrar los primeros 20 con código
    cursor.execute("""
        SELECT codigo, nombre, cuit 
        FROM erp_terceros 
        WHERE enterprise_id = 0 AND es_proveedor = 1 AND codigo IS NOT NULL
        ORDER BY codigo
        LIMIT 20
    """)
    
    proveedores = cursor.fetchall()
    print(f"\nProveedores con código ({len(proveedores)}):")
    for p in proveedores:
        print(f"  {p['codigo']:15} | {p['nombre']:40} | {p['cuit']}")
