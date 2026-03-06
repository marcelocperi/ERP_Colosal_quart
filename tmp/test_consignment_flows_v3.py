
import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor
from services.consignment_service import ConsignmentService

def test_consignment_flows():
    """
    Test de flujos industriales (Fase 1.5).
    """
    try:
        enterprise_id = 0
        user_id = 1
        
        # 1. Crear consignación
        with get_db_cursor(dictionary=True) as cursor:
            # Buscar IDs válidos
            cursor.execute("SELECT id FROM erp_terceros WHERE es_proveedor = 1 LIMIT 1")
            faz = cursor.fetchone()
            fazonero_id = faz['id'] if faz else 1
            
            cursor.execute("SELECT id FROM erp_terceros WHERE es_cliente = 1 LIMIT 1")
            cli = cursor.fetchone()
            cliente_id = cli['id'] if cli else 2

            cursor.execute("SELECT id FROM stk_articulos LIMIT 2")
            arts = cursor.fetchall()
            insumo_id = arts[0]['id']
            producto_id = arts[1]['id']

        print(f"Usando Insumo: {insumo_id}, Producto: {producto_id}")

        # Crear
        cid = ConsignmentService.crear_consignacion(
            enterprise_id, 'TENENCIA_CLIENTE', cliente_id,
            [{'articulo_id': producto_id, 'cantidad': 50.0, 'costo': 500.0}],
            ref_doc='TEST-ENRIQUE', user_id=user_id
        )
        print(f"Consignación #{cid} creada.")

        # Consultar items con UNA NUEVA CONEXIÓN para forzar lectura de disco
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM cmp_items_consignacion WHERE consignacion_id = %s", (cid,))
            items = cursor.fetchall()
            print(f"Items encontrados para #{cid}: {len(items)}")
            
            if items:
                item_id = items[0]['id']
                print(f"Liquidando 10u del item #{item_id}...")
                ConsignmentService.liquidar_evento(enterprise_id, item_id, 10.0, 'VENTA', user_id=user_id)
                
                # Verificar saldo
                cursor.execute("SELECT cantidad_original, cantidad_consumida FROM cmp_items_consignacion WHERE id = %s", (item_id,))
                status = cursor.fetchone()
                print(f"STATUS FINAL: Enviado {status['cantidad_original']}, Consumido {status['cantidad_consumida']}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_consignment_flows()
