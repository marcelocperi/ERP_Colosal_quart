from database import get_db_cursor

def setup_test():
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Configurar empresa (Dietética Palomar) como agente de ARBA (BUENOS AIRES)
        print("Configuring enterprise agent status...")
        cursor.execute("DELETE FROM sys_enterprises_fiscal WHERE enterprise_id = 1 AND jurisdiccion = 'BUENOS AIRES'")
        cursor.execute("""
            INSERT INTO sys_enterprises_fiscal (enterprise_id, tipo, jurisdiccion, activo)
            VALUES (1, 'PERCEPCION', 'BUENOS AIRES', 1)
        """)
        
        # 2. Buscar o Crear un cliente de prueba
        cuit_test = '30111111118'
        print(f"Checking for test client with CUIT {cuit_test}...")
        cursor.execute("SELECT id FROM erp_terceros WHERE cuit = %s AND enterprise_id = 1", (cuit_test,))
        row = cursor.fetchone()
        
        if row:
            client_id = row['id']
            print(f"Test client found with ID {client_id}")
        else:
            print("Creating test client...")
            cursor.execute("""
                INSERT INTO erp_terceros (enterprise_id, nombre, cuit, es_cliente, es_proveedor, activo, tipo_responsable)
                VALUES (1, 'CLIENTE TEST PERCEPCIONES S.A.', %s, 1, 0, 1, 'Responsable Inscripto')
            """, (cuit_test,))
            client_id = cursor.lastrowid
            print(f"Test client created with ID {client_id}")
            
        # 3. Asignar alícuota de percepción para BUENOS AIRES
        print("Configuring client fiscal data...")
        cursor.execute("DELETE FROM erp_datos_fiscales WHERE tercero_id = %s AND jurisdiccion = 'BUENOS AIRES'", (client_id,))
        cursor.execute("""
            INSERT INTO erp_datos_fiscales (enterprise_id, tercero_id, impuesto, jurisdiccion, condicion, numero_inscripcion, alicuota)
            VALUES (1, %s, 'IIBB', 'BUENOS AIRES', 'Inscripto', %s, 3.5)
        """, (client_id, cuit_test))
        
        # 4. Asegurar que el cliente tenga una dirección
        print("Configuring client address...")
        cursor.execute("DELETE FROM erp_direcciones WHERE tercero_id = %s", (client_id,))
        cursor.execute("""
            INSERT INTO erp_direcciones (enterprise_id, tercero_id, etiqueta, calle, numero, localidad, provincia, es_fiscal)
            VALUES (1, %s, 'Casa Central', 'Calle Falsa', '123', 'La Plata', 'BUENOS AIRES', 1)
        """, (client_id,))

    print(f"\n✅ TEST SETUP COMPLETE.")
    print(f"👉 CLIENT: CLIENTE TEST PERCEPCIONES S.A.")
    print(f"👉 CUIT: {cuit_test}")
    print(f"👉 AGENT JURISDICTION: BUENOS AIRES")
    print(f"👉 RATE: 3.5%")

if __name__ == '__main__':
    setup_test()
