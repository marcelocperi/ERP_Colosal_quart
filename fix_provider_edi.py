from database import get_db_cursor

with get_db_cursor() as cursor:
    # Revertir a enterprise_id = 0 y solo marcar como proveedor
    cursor.execute("""
        UPDATE erp_terceros 
        SET es_proveedor = 1, 
            enterprise_id = 0,
            codigo = 'EDI-0001'
        WHERE cuit = '30452157779'
    """)
    
    print(f"✅ Proveedor actualizado:")
    print(f"   - Marcado como proveedor (es_proveedor = 1)")
    print(f"   - Mantenido en enterprise_id = 0 (Empresa Global)")
    print(f"   - Código: EDI-0001")
    
    # Verificar
    cursor.execute("SELECT id, codigo, nombre, cuit, es_proveedor, enterprise_id FROM erp_terceros WHERE cuit = '30452157779'")
    result = cursor.fetchone()
    print(f"\n✅ Verificación:")
    print(f"   ID: {result[0]}")
    print(f"   Código: {result[1]}")
    print(f"   Nombre: {result[2]}")
    print(f"   Es Proveedor: {result[4]}")
    print(f"   Enterprise: {result[5]}")
