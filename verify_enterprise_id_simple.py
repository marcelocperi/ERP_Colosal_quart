from database import get_db_cursor

TABLES_TO_CHECK = [
    'stk_logisticas',
    'stk_transferencias', 
    'stk_items_transferencia',
    'stk_inventarios',
    'stk_items_inventario'
]

print("=" * 70)
print("VERIFICACION DE ENTERPRISE_ID EN TABLAS DE STOCK")
print("=" * 70)

with get_db_cursor(dictionary=True) as cursor:
    for table in TABLES_TO_CHECK:
        print(f"\nTabla: {table}")
        print("-" * 70)
        
        try:
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            
            has_enterprise_id = False
            for col in columns:
                if col['Field'] == 'enterprise_id':
                    has_enterprise_id = True
                    print(f"[OK] TIENE enterprise_id")
                    print(f"   - Tipo: {col['Type']}")
                    print(f"   - Null: {col['Null']}")
                    print(f"   - Key: {col['Key']}")
                    break
            
            if not has_enterprise_id:
                print(f"[FALTA] NO TIENE enterprise_id")
                print("   Columnas disponibles:")
                for col in columns[:5]:  # Solo primeras 5 columnas
                    print(f"      - {col['Field']} ({col['Type']})")
                    
        except Exception as e:
            print(f"[ERROR] al consultar tabla: {e}")

print("\n" + "=" * 70)
print("FIN DE VERIFICACION")
print("=" * 70)
