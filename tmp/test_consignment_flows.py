
import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor
from services.consignment_service import ConsignmentService

def test_consignment_flows():
    """
    Simulación de flujos industriales:
    1. Envío a Fazón (Materia Prima).
    2. Tenencia a Cliente (Producto Terminado).
    3. Liquidación de Consumos.
    """
    try:
        enterprise_id = 0
        user_id = 1
        
        with get_db_cursor(dictionary=True) as cursor:
            # Seleccionar Terceros (Fazonero y Cliente)
            cursor.execute("SELECT id, nombre FROM erp_terceros WHERE (enterprise_id=0 OR enterprise_id=1) LIMIT 2")
            terceros = cursor.fetchall()
            fazonero_id = terceros[0]['id']
            cliente_id = terceros[1]['id']

            # Artículos (Insumo y Producto)
            cursor.execute("SELECT id, nombre FROM stk_articulos WHERE (enterprise_id=0 OR enterprise_id=1) LIMIT 2")
            arts = cursor.fetchall()
            insumo_id = arts[0]['id']
            producto_id = arts[1]['id']

            print("--- ESCENARIO 1: Envío a Fazón (Producción Externa) ---")
            cid1 = ConsignmentService.crear_consignacion(
                enterprise_id, 'EXTERNA_SALIDA', fazonero_id, 
                [{'articulo_id': insumo_id, 'cantidad': 100.0, 'costo': 10.0}],
                ref_doc='REM-F001', user_id=user_id
            )
            print(f"Consignación Fazón #{cid1} creada.")

            print("--- ESCENARIO 2: Tenencia para Desarrollo de Cliente ---")
            cid2 = ConsignmentService.crear_consignacion(
                enterprise_id, 'TENENCIA_CLIENTE', cliente_id,
                [{'articulo_id': producto_id, 'cantidad': 50.0, 'costo': 500.0}],
                ref_doc='REM-T001', user_id=user_id
            )
            print(f"Tenencia Cliente #{cid2} creada.")

            # Consultar pendiendtes
            pendientes = ConsignmentService.get_stock_en_consignacion(enterprise_id)
            print(f"Stock Pendiente en Consignación (CISA Check): {len(pendientes)} items.")

            print("--- ESCENARIO 3: Liquidación de Venta (Tenencia) ---")
            # Obtenemos el item de la consignación 2
            cursor.execute("SELECT id FROM cmp_items_consignacion WHERE consignacion_id = %s", (cid2,))
            item_id = cursor.fetchone()['id']
            
            # El cliente vendió 10u. Liquidamos.
            res = ConsignmentService.liquidar_evento(enterprise_id, item_id, 10.0, 'VENTA', comprobante_id=999, user_id=user_id)
            if res:
                print(f"Liquidada venta de 10u. El cliente ahora tiene 40u pendientes.")

            print("FLUJO DE PRUEBA COMPLETADO.")

    except Exception as e:
        print(f"Error en flujos de consignación: {e}")

if __name__ == "__main__":
    test_consignment_flows()
