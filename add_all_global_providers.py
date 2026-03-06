from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    # 1. Ver cuántos terceros hay en enterprise_id = 0 que NO son proveedores
    cursor.execute("""
        SELECT COUNT(*) as total 
        FROM erp_terceros 
        WHERE enterprise_id = 0 AND (es_proveedor = 0 OR es_proveedor IS NULL)
    """)
    pendientes = cursor.fetchone()['total']
    
    print(f"📊 Terceros en enterprise_id=0 sin flag de proveedor: {pendientes}")
    
    if pendientes > 0:
        # 2. Mostrar algunos ejemplos antes de actualizar
        cursor.execute("""
            SELECT id, codigo, nombre, cuit, es_proveedor 
            FROM erp_terceros 
            WHERE enterprise_id = 0 AND (es_proveedor = 0 OR es_proveedor IS NULL)
            LIMIT 10
        """)
        ejemplos = cursor.fetchall()
        
        print("\n📋 Ejemplos de terceros que se marcarán como proveedores:")
        for t in ejemplos:
            print(f"   ID {t['id']}: {t['nombre']} (CUIT: {t['cuit']}, Código: {t['codigo']})")
        
        # 3. Actualizar todos
        cursor.execute("""
            UPDATE erp_terceros 
            SET es_proveedor = 1
            WHERE enterprise_id = 0 AND (es_proveedor = 0 OR es_proveedor IS NULL)
        """)
        
        print(f"\n✅ {pendientes} terceros actualizados como proveedores globales")
    
    # 4. Verificar resultado final
    cursor.execute("""
        SELECT COUNT(*) as total 
        FROM erp_terceros 
        WHERE enterprise_id = 0 AND es_proveedor = 1
    """)
    total_proveedores = cursor.fetchone()['total']
    
    print(f"\n📊 Total de proveedores globales (enterprise_id=0): {total_proveedores}")
    
    # 5. Mostrar algunos proveedores globales con código
    cursor.execute("""
        SELECT id, codigo, nombre, cuit 
        FROM erp_terceros 
        WHERE enterprise_id = 0 AND es_proveedor = 1 AND codigo IS NOT NULL
        ORDER BY codigo
        LIMIT 10
    """)
    con_codigo = cursor.fetchall()
    
    if con_codigo:
        print(f"\n✅ Proveedores globales con código asignado:")
        for p in con_codigo:
            print(f"   {p['codigo']}: {p['nombre']} (CUIT: {p['cuit']})")
