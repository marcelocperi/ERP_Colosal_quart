import json
from database import get_db_cursor

def add_pending_to_roadmap():
    print("Registrando nuevos pendientes en sys_roadmap_decisions...")
    
    with get_db_cursor() as cursor:
        data = [
            (0, 'Finanzas', 'Analítica', 'Análisis de Variaciones de Precio', 'Reporte comparativo entre precios cotizados en RFQ y precios finales de compra en PO.', 'Pendiente', 1, 1),
            (0, 'Seguridad', 'Auditoría', 'Gestión de EPP y Checklist', 'Registro de entrega de Elementos de Protección Personal y validación previa a despachos peligrosos.', 'Pendiente', 1, 1),
            (0, 'Stock', 'Graneles', 'Integración de Pesaje y Tolerancias', 'Captura de balanza para graneles con cálculo de merma técnica por diferencia de pesaje.', 'Pendiente', 1, 1),
            (0, 'Stock', 'Catalogación', 'UI de Equivalencias SKU Dual', 'Búsqueda bidireccional código proveedor <-> SKU propio en consultas.', 'Pendiente', 1, 1)
        ]
        
        cursor.executemany("""
            INSERT INTO sys_roadmap_decisions 
            (enterprise_id, modulo, subcategoria, funcionalidad, descripcion_ampliada, decision, dt_user_created, dt_user_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, data)
        print(f"✅ Se registraron {len(data)} ítems adicionales como 'Pendiente'.")

if __name__ == "__main__":
    add_pending_to_roadmap()
