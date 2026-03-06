import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    # Verificar si la tabla existe
    cursor.execute("""
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'libros_detalles'
    """)
    
    table_exists = cursor.fetchone()
    
    if not table_exists:
        print("❌ La tabla 'libros_detalles' NO existe en la base de datos")
    else:
        print("✓ La tabla 'libros_detalles' existe\n")
        
        # Obtener estructura de la tabla
        cursor.execute("DESCRIBE libros_detalles")
        columns = cursor.fetchall()
        
        print("ESTRUCTURA DE LA TABLA:")
        print("-" * 80)
        for col in columns:
            print(f"  {col['Field']:20} | {col['Type']:20} | Null: {col['Null']:3} | Key: {col['Key']:3} | Default: {col['Default']}")
        
        # Contar registros
        cursor.execute("SELECT COUNT(*) as total FROM libros_detalles")
        count = cursor.fetchone()['total']
        print(f"\n📊 Total de registros: {count}")
        
        if count > 0:
            # Mostrar algunos ejemplos
            cursor.execute("SELECT * FROM libros_detalles LIMIT 5")
            samples = cursor.fetchall()
            
            print("\n📖 EJEMPLOS DE REGISTROS:")
            print("-" * 80)
            for i, sample in enumerate(samples, 1):
                print(f"\nRegistro #{i}:")
                for key, value in sample.items():
                    if value is not None:
                        # Truncar valores largos
                        str_value = str(value)
                        if len(str_value) > 100:
                            str_value = str_value[:100] + "..."
                        print(f"  {key}: {str_value}")
    
    conn.close()
    
except mariadb.Error as e:
    print(f"❌ Error de base de datos: {e}")
except Exception as e:
    print(f"❌ Error: {e}")
