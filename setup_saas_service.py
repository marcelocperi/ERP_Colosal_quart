from database import get_db_cursor

def setup_saas_service():
    with get_db_cursor() as cursor:
        # 1. Ensure 'Servicio' type
        cursor.execute("INSERT IGNORE INTO stk_tipos_articulo (enterprise_id, nombre, descripcion) VALUES (1, 'Servicio', 'Servicios de Software')")
        cursor.execute("SELECT id FROM stk_tipos_articulo WHERE enterprise_id = 1 AND nombre = 'Servicio'")
        tipo_id = cursor.fetchone()[0]
        
        # 2. Add the service
        cursor.execute("""
            INSERT IGNORE INTO stk_articulos (enterprise_id, codigo, nombre, descripcion, tipo_articulo_id, tipo_articulo, precio_venta, activo)
            VALUES (1, 'SERV-SAAS', 'Alquiler de Software SaaS', 'Servicio mensual de suscripción a la plataforma', %s, 'Servicio', 0.00, 1)
        """, (tipo_id,))
        print("SaaS Rental Service article created in Enterprise 1.")

if __name__ == "__main__":
    setup_saas_service()
