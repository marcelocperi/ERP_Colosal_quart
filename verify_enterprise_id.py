from database import get_db_cursor

TABLES_TO_CHECK = [
    'stk_logisticas',
    'stk_transferencias', 
    'stk_items_transferencia',
    'stk_inventarios',
    'stk_items_inventario'
]

print("=" * 70)
print("VERIFICACIÓN DE ENTERPRISE_ID EN TABLAS DE STOCK")
print("=" * 70)

with get_db_cursor(dictionary=True) as cursor:
    for table in TABLES_TO_CHECK:
        print(f"\n📋 Tabla: {table}")
        print("-" * 70)
        
        try:
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            
            has_enterprise_id = False
            for col in columns:
                if col['Field'] == 'enterprise_id':
                    has_enterprise_id = True
                    print(f"✅ TIENE enterprise_id")
                    print(f"   - Tipo: {col['Type']}")
                    print(f"   - Null: {col['Null']}")
                    print(f"   - Key: {col['Key']}")
                    print(f"   - Default: {col['Default']}")
                    break
            
            if not has_enterprise_id:
                print(f"❌ NO TIENE enterprise_id")
                print("   🔧 Columnas disponibles:")
                for col in columns:
                    print(f"      - {col['Field']} ({col['Type']})")
                    
        except Exception as e:
            print(f"⚠️  Error al consultar tabla: {e}")

print("\n" + "=" * 70)
print("FIN DE VERIFICACIÓN")
print("=" * 70)
