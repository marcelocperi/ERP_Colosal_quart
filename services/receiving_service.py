# -*- coding: utf-8 -*-
from database import get_db_cursor
import datetime

class ReceivingService:
    @staticmethod
    async def get_po_for_blind_receiving(enterprise_id, po_id):
        """
        Devuelve la Orden de Compra pero OCULTA las cantidades originales a recibir (Blind Receiving).
        Solo un usuario con permisos de Depósito debería acceder a esto.
        """
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("""
                SELECT o.id, o.fecha_emision, p.nombre as proveedor_nombre, o.estado
                FROM cmp_ordenes_compra o
                JOIN erp_terceros p ON o.proveedor_id = p.id
                WHERE o.id = %s AND o.enterprise_id = %s AND o.estado IN ('ENVIADA_PROVEEDOR', 'EN_TRANSITO', 'RECIBIDA_PARCIAL', 'ENVIADA_TESORERIA')
            """, (po_id, enterprise_id))
            po = await cursor.fetchone()
            
            if not po:
                return None
                
            # Ocultamos cantidad_solicitada explícitamente y enviamos lo que falta recibir
            await cursor.execute("""
                SELECT d.id as detalle_id, d.articulo_id, a.nombre as articulo_nombre, a.codigo as articulo_codigo,
                       d.cantidad_solicitada, d.cantidad_recibida,
                       (d.cantidad_solicitada - d.cantidad_recibida) as pendiente_aparente
                FROM cmp_detalles_orden d
                JOIN stk_articulos a ON d.articulo_id = a.id
                WHERE d.orden_id = %s AND d.enterprise_id = %s
            """, (po_id, enterprise_id))
            items = await cursor.fetchall()
            
            # BLIND RECEIVING: El operador de depósito no debe ver 'cantidad_solicitada' ni 'pendiente_aparente' 
            # en la interfaz final, solo el nombre del artículo. Pero los enviamos para validación en el backend.
            po['items'] = items
            return po

    @staticmethod
    async def process_blind_receipt(enterprise_id, user_id, po_id, rec_data):
        """
        Procesa el conteo físico ingresado a ciegas por el operador.
        Compara las cantidades reales vs las solicitadas en la PO.
        """
        discrepancy_detected = False
        now = datetime.datetime.now()
        
        async with get_db_cursor(dictionary=True) as cursor:
            # 1. Crear cabecera de Recepción
            await cursor.execute("""
                INSERT INTO stk_recepciones (enterprise_id, orden_compra_id, numero_remito_proveedor, fecha_recepcion, recibido_por, observaciones)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (enterprise_id, po_id, rec_data.get('remito', ''), now.date(), user_id, rec_data.get('observaciones', '')))
            recepcion_id = cursor.lastrowid
            
            po_fully_received = True

            # 2. Procesar ítems recibidos
            for detalle_id, cant_recibida in rec_data.get('items', {}).items():
                cant_recibida = float(cant_recibida)
                
                # Buscar detalle original
                await cursor.execute("""
                    SELECT articulo_id, cantidad_solicitada, cantidad_recibida 
                    FROM cmp_detalles_orden 
                    WHERE id = %s AND enterprise_id = %s
                """, (detalle_id, enterprise_id))
                linea_po = await cursor.fetchone()
                
                if not linea_po:
                    continue
                    
                total_ya_recibido = float(linea_po['cantidad_recibida']) + cant_recibida
                cant_solicitada = float(linea_po['cantidad_solicitada'])
                
                # Flag CISA: Evaluar diferencias ocultas
                has_diff = 1 if total_ya_recibido != cant_solicitada else 0
                if has_diff:
                    discrepancy_detected = True
                    
                if total_ya_recibido < cant_solicitada:
                    po_fully_received = False
                    
                # Guardar el detalle de recepción
                await cursor.execute("""
                    INSERT INTO stk_detalles_recepcion (enterprise_id, recepcion_id, detalle_orden_id, articulo_id, cantidad_recibida, diferencia_detectada)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (enterprise_id, recepcion_id, detalle_id, linea_po['articulo_id'], cant_recibida, has_diff))
                
                # Add to stock
                await cursor.execute("""
                    UPDATE stk_articulos 
                    SET stock_actual = stock_actual + %s, fecha_ultima_actualizacion = NOW()
                    WHERE id = %s AND enterprise_id = %s
                """, (cant_recibida, linea_po['articulo_id'], enterprise_id))

                # Actualizar saldo pendiente en la Orden de Compra
                await cursor.execute("""
                    UPDATE cmp_detalles_orden 
                    SET cantidad_recibida = %s 
                    WHERE id = %s AND enterprise_id = %s
                """, (total_ya_recibido, detalle_id, enterprise_id))
                
            # 3. Actualizar estado de la PO principal
            nuevo_estado_po = 'RECIBIDA_TOTAL' if po_fully_received else 'RECIBIDA_PARCIAL'
            await cursor.execute("""
                UPDATE cmp_ordenes_compra 
                SET estado = %s 
                WHERE id = %s AND enterprise_id = %s
            """, (nuevo_estado_po, po_id, enterprise_id))
            
            return {
                'success': True,
                'recepcion_id': recepcion_id,
                'discrepancy': discrepancy_detected,
                'estado_final_po': nuevo_estado_po
            }

    @staticmethod
    async def match_invoice_vs_receipt(enterprise_id, po_id, items_facturados):
        """
        3-Way Match: Asegura que la Factura (Tesorería) = Recepción (Depósito) = PO (Compras)
        """
        discrepancies = []
        async with get_db_cursor(dictionary=True) as cursor:
            for item in items_facturados:
                detalle_id = item['detalle_po_id']
                cant_facturada = float(item['cantidad'])
                precio_facturado = float(item['precio'])
                
                await cursor.execute("""
                    SELECT cantidad_solicitada, cantidad_recibida, precio_unitario 
                    FROM cmp_detalles_orden 
                    WHERE id = %s AND enterprise_id = %s
                """, (detalle_id, enterprise_id))
                linea = await cursor.fetchone()
                
                if not linea:
                    continue
                    
                if cant_facturada > float(linea['cantidad_recibida']):
                    discrepancies.append(f"Cant. Facturada mayor a la Recibida (Fact: {cant_facturada}, Rec: {linea['cantidad_recibida']})")
                    
                # Tolerancia del 1% en precio, por ejemplo 
                precio_po = float(linea['precio_unitario'])
                if precio_po > 0 and precio_facturado > (precio_po * 1.01):
                    discrepancies.append(f"Precio Facturado superior al pactado en la PO (Fact: ${precio_facturado}, PO: ${precio_po})")

        return {
            'success': len(discrepancies) == 0,
            'discrepancies': discrepancies
        }
