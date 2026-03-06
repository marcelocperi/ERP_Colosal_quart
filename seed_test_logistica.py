from database import get_db_cursor

def seed_test_logistica():
    print("Registrando logística de prueba...")
    with get_db_cursor() as cursor:
        # Usamos enterprise_id = 1 para la prueba local
        sql = """
            INSERT INTO stk_logisticas (enterprise_id, nombre, cuit, calle, numero, localidad, provincia, direccion, email, telefono, activo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            1, 
            'Logística Federal S.A.', 
            '30-71456789-2', 
            'Av. de Circunvalación', 
            '4500', 
            'Lanús', 
            'Buenos Aires', 
            'Av. de Circunvalación 4500 - Lanús, Buenos Aires', 
            'contacto@logisticafederal.com.ar', 
            '011-4241-9999', 
            1
        )
        cursor.execute(sql, params)
    print("✅ Empresa 'Logística Federal S.A.' registrada correctamente.")

if __name__ == "__main__":
    seed_test_logistica()
