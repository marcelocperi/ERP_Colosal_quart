from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    # Buscar el proveedor específico por CUIT
    cursor.execute("SELECT id, codigo, nombre, cuit, es_proveedor, activo, enterprise_id FROM erp_terceros WHERE cuit = '30452157779'")
    proveedor = cursor.fetchone()
    
    if proveedor:
        print(f"✅ Proveedor encontrado:")
        print(f"   ID: {proveedor['id']}")
        print(f"   Código: '{proveedor['codigo']}'")
        print(f"   Nombre: {proveedor['nombre']}")
        print(f"   CUIT: {proveedor['cuit']}")
        print(f"   Es Proveedor: {proveedor['es_proveedor']}")
        print(f"   Activo: {proveedor['activo']}")
        print(f"   Enterprise ID: {proveedor['enterprise_id']}")
    else:
        print("❌ No se encontró proveedor con ese CUIT")
    
    # Buscar por código
    cursor.execute("SELECT id, codigo, nombre, cuit FROM erp_terceros WHERE codigo = 'EDI-0001'")
    por_codigo = cursor.fetchone()
    
    if por_codigo:
        print(f"\n✅ Búsqueda por código 'EDI-0001':")
        print(f"   ID: {por_codigo['id']}, Nombre: {por_codigo['nombre']}, CUIT: {por_codigo['cuit']}")
