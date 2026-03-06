from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    print("=" * 80)
    print("BÚSQUEDA EXHAUSTIVA EN TABLA: erp_terceros")
    print("=" * 80)
    
    # 1. Estadísticas generales
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN es_proveedor = 1 THEN 1 ELSE 0 END) as proveedores,
            SUM(CASE WHEN es_proveedor = 1 AND (codigo IS NULL OR codigo = '') THEN 1 ELSE 0 END) as sin_codigo,
            SUM(CASE WHEN es_proveedor = 1 AND codigo IS NOT NULL AND codigo != '' THEN 1 ELSE 0 END) as con_codigo
        FROM erp_terceros
        WHERE enterprise_id = 0
    """)
    
    stats = cursor.fetchone()
    print(f"\n📊 ESTADÍSTICAS (enterprise_id = 0):")
    print(f"   Total terceros: {stats['total']}")
    print(f"   Marcados como proveedores: {stats['proveedores']}")
    print(f"   Proveedores SIN código: {stats['sin_codigo']}")
    print(f"   Proveedores CON código: {stats['con_codigo']}")
    
    # 2. Listar TODOS los proveedores sin código
    cursor.execute("""
        SELECT id, nombre, cuit, codigo, enterprise_id
        FROM erp_terceros
        WHERE enterprise_id = 0 
          AND es_proveedor = 1 
          AND (codigo IS NULL OR codigo = '')
        ORDER BY nombre
    """)
    
    sin_codigo = cursor.fetchall()
    
    if sin_codigo:
        print(f"\n⚠️  PROVEEDORES SIN CÓDIGO ({len(sin_codigo)}):")
        print("-" * 80)
        for i, p in enumerate(sin_codigo, 1):
            print(f"{i:3}. ID:{p['id']:4} | {p['nombre']:45} | CUIT: {p['cuit']}")
    
    # 3. Buscar específicamente "Litoral Insumos"
    print("\n" + "=" * 80)
    print("BÚSQUEDA ESPECÍFICA: 'Litoral'")
    print("=" * 80)
    
    cursor.execute("""
        SELECT id, codigo, nombre, cuit, es_proveedor, enterprise_id
        FROM erp_terceros
        WHERE (nombre LIKE '%Litoral%' OR nombre LIKE '%LITORAL%' OR nombre LIKE '%litoral%')
    """)
    
    litorales = cursor.fetchall()
    
    if litorales:
        print(f"\n✅ Encontrados {len(litorales)} registros con 'Litoral':")
        for lit in litorales:
            print(f"   ID {lit['id']}: {lit['nombre']}")
            print(f"      CUIT: {lit['cuit']}, Código: {lit['codigo']}, Proveedor: {lit['es_proveedor']}, Enterprise: {lit['enterprise_id']}")
    else:
        print("❌ No se encontraron registros con 'Litoral' en el nombre")
