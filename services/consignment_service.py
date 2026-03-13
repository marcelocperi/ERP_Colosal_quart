
from database import get_db_cursor

class ConsignmentService:
    """
    Gestión de Inventario en Consignación y Tenencia (Phase 1.5).
    """

    @staticmethod
    async def crear_consignacion(enterprise_id, tipo, tercero_id, items, ref_doc=None, user_id=None):
        """
        Registra un envío o recepción en consignación.
        items: [{'articulo_id': 1, 'cantidad': 100, 'costo': 50.0}, ...]
        """
        async with get_db_cursor() as cursor:
            # 1. Cabecera
            await cursor.execute("""
                INSERT INTO cmp_consignaciones (enterprise_id, tipo, tercero_id, referencia_doc, user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (enterprise_id, tipo, tercero_id, ref_doc, user_id))
            cons_id = cursor.lastrowid

            # 2. Detalles
            for it in items:
                await cursor.execute("""
                    INSERT INTO cmp_items_consignacion (consignacion_id, articulo_id, cantidad_original, costo_unitario_pactado, user_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (cons_id, it['articulo_id'], it['cantidad'], it.get('costo', 0), user_id))
            
            return cons_id

    @staticmethod
    async def liquidar_evento(enterprise_id, consignacion_item_id, cantidad, tipo_evento, comprobante_id=None, user_id=None):
        """
        Registra el consumo (Venta/Producción) de un item consignado.
        Actualiza el estado del stock externo.
        """
        async with get_db_cursor() as cursor:
            # 1. Validar remanente
            await cursor.execute("""
                SELECT cantidad_original, cantidad_consumida, cantidad_devuelta 
                FROM cmp_items_consignacion WHERE id = %s
            """, (consignacion_item_id,))
            res = await cursor.fetchone()
            if not res: return False
            
            total_orig, total_cons, total_dev = res
            disponible = total_orig - total_cons - total_dev
            
            if float(cantidad) > float(disponible):
                raise ValueError(f"Cantidad a liquidar ({cantidad}) excede el disponible consignado ({disponible}).")

            # 2. Registrar Liquidación
            await cursor.execute("""
                INSERT INTO cmp_liquidaciones_consignacion (consignacion_item_id, cantidad_liquidada, tipo_evento, comprobante_id, user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (consignacion_item_id, cantidad, tipo_evento, comprobante_id, user_id))

            # 3. Actualizar Item
            await cursor.execute("""
                UPDATE cmp_items_consignacion 
                SET cantidad_consumida = cantidad_consumida + %s 
                WHERE id = %s
            """, (cantidad, consignacion_item_id))
            
            print(f"[CONSIGNACION] Liquidando {cantidad} del item {consignacion_item_id}. Filas afectadas: {cursor.rowcount}")
            
            return True

    @staticmethod
    async def get_stock_en_consignacion(enterprise_id, tercero_id=None, tipo=None):
        """
        Retorna el listado de pendientes de liquidación en poder de terceros o proveedores.
        """
        async with get_db_cursor(dictionary=True) as cursor:
            sql = """
                SELECT 
                    c.id as cons_id, c.tipo, t.nombre as tercero, 
                    i.articulo_id, a.nombre as articulo, 
                    (i.cantidad_original - i.cantidad_consumida - i.cantidad_devuelta) as pendiente,
                    i.costo_unitario_pactado as valor_unitario
                FROM cmp_consignaciones c
                JOIN cmp_items_consignacion i ON c.id = i.consignacion_id
                JOIN erp_terceros t ON c.tercero_id = t.id
                JOIN stk_articulos a ON i.articulo_id = a.id
                WHERE c.enterprise_id = %s AND c.estado IN ('ABIERTA', 'CERRADA_PARCIAL')
            """
            params = [enterprise_id]
            if tercero_id:
                sql += " AND c.tercero_id = %s"
                params.append(tercero_id)
            if tipo:
                sql += " AND c.tipo = %s"
                params.append(tipo)
            
            await cursor.execute(sql, tuple(params))
            return await cursor.fetchall()
