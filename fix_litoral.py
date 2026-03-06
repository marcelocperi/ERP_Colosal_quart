from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    # Buscar por CUIT (sin guiones)
    cuit = '30361966614'
    
    cursor.execute("""
        SELECT id, codigo, nombre, cuit, es_proveedor, enterprise_id 
        FROM erp_terceros 
        WHERE cuit = %s OR cuit = '30-36196661-4'
    """, (cuit,))
    
    proveedor = cursor.fetchone()
    
    if proveedor:
        print(f"✅ Proveedor encontrado:")
        print(f"   ID: {proveedor['id']}")
        print(f"   Nombre: {proveedor['nombre']}")
        print(f"   CUIT: {proveedor['cuit']}")
        print(f"   Código actual: {proveedor['codigo']}")
        print(f"   Es Proveedor: {proveedor['es_proveedor']}")
        print(f"   Enterprise: {proveedor['enterprise_id']}")
        
        # Asignar código LIT-0377
        cursor.execute("""
            UPDATE erp_terceros 
            SET codigo = 'LIT-0377',
                es_proveedor = 1
            WHERE id = %s
        """, (proveedor['id'],))
        
        print(f"\n✅ Actualizado:")
        print(f"   Código: LIT-0377")
        print(f"   es_proveedor: 1")
        
    else:
        print(f"❌ No se encontró proveedor con CUIT {cuit}")
        
        # Buscar por nombre
        cursor.execute("""
            SELECT id, codigo, nombre, cuit 
            FROM erp_terceros 
            WHERE nombre LIKE '%Litoral%' OR nombre LIKE '%LITORAL%'
        """)
        
        similares = cursor.fetchall()
        if similares:
            print(f"\n📋 Proveedores similares encontrados:")
            for s in similares:
                print(f"   ID {s['id']}: {s['nombre']} (CUIT: {s['cuit']}, Código: {s['codigo']})")
