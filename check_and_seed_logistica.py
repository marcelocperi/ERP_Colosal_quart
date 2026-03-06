from database import get_db_cursor

print("Verificando estado de tabla stk_logisticas...")
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("DESCRIBE stk_logisticas")
    print("\nEstructura de tabla:")
    for col in cursor.fetchall():
        print(f"  - {col['Field']} ({col['Type']})")
    
    cursor.execute("SELECT COUNT(*) as total FROM stk_logisticas WHERE enterprise_id = 1")
    count = cursor.fetchone()['total']
    print(f"\n📊 Total de logísticas registradas para empresa 1: {count}")
    
    if count == 0:
        print("\n⚙️  Insertando logística de prueba...")
        try:
            cursor.execute("""
                INSERT INTO stk_logisticas 
                (enterprise_id, nombre, cuit, calle, numero, localidad, provincia, direccion, email, telefono, activo)
                VALUES 
                (1, 'Logística Federal S.A.', '30-71456789-2', 'Av. de Circunvalación', '4500', 
                 'Lanús', 'Buenos Aires', 'Av. de Circunvalación 4500 - Lanús, Buenos Aires', 
                 'contacto@logisticafederal.com.ar', '011-4241-9999', 1)
            """)
            print("✅ Logística registrada exitosamente!")
        except Exception as e:
            print(f"❌ Error al insertar: {e}")
    else:
        print("ℹ️  Ya existen logísticas registradas.")
        cursor.execute("SELECT id, nombre, cuit FROM stk_logisticas WHERE enterprise_id = 1")
        for row in cursor.fetchall():
            print(f"  • {row['nombre']} (CUIT: {row['cuit']}) - ID: {row['id']}")
