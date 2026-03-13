
from database import get_db_cursor
from datetime import datetime

class SourcingService:
    """
    Servicio encargado de aplicar las reglas de Sourcing (Fase 1.2).
    Determina qué proveedor es el más conveniente o habitual para un artículo dado.
    """

    @staticmethod
    async def get_best_option(enterprise_id, articulo_id, strategy='BEST_PRICE'):
        """
        Retorna el mejor proveedor para un artículo basado en una estrategia.
        Estrategias:
        - BEST_PRICE: El que tenga el precio_referencia más bajo en cmp_articulos_proveedores.
        - LAST_RECEPTION: El que proveyó la última factura de compra.
        - HABITUAL: El marcado como es_habitual=1.
        """
        async with get_db_cursor(dictionary=True) as cursor:
            if strategy == 'HABITUAL':
                sql = """
                    SELECT ap.*, t.nombre as proveedor_nombre, o.nombre as origen_nombre
                    FROM cmp_articulos_proveedores ap
                    JOIN erp_terceros t ON ap.proveedor_id = t.id
                    LEFT JOIN cmp_sourcing_origenes o ON ap.origen_id = o.id
                    WHERE ap.enterprise_id = %s AND ap.articulo_id = %s AND ap.es_habitual = 1
                    LIMIT 1
                """
                await cursor.execute(sql, (enterprise_id, articulo_id))
                return await cursor.fetchone()

            elif strategy == 'BEST_PRICE':
                sql = """
                    SELECT ap.*, t.nombre as proveedor_nombre, o.nombre as origen_nombre
                    FROM cmp_articulos_proveedores ap
                    JOIN erp_terceros t ON ap.proveedor_id = t.id
                    LEFT JOIN cmp_sourcing_origenes o ON ap.origen_id = o.id
                    WHERE ap.enterprise_id = %s AND ap.articulo_id = %s
                    ORDER BY ap.precio_referencia ASC
                    LIMIT 1
                """
                await cursor.execute(sql, (enterprise_id, articulo_id))
                return await cursor.fetchone()

            elif strategy == 'LAST_RECEPTION':
                # Buscamos en el detalle de comprobantes de compra
                sql = """
                    SELECT 
                        t.id as proveedor_id, t.nombre as proveedor_nombre, 
                        cd.precio_unitario as precio_referencia,
                        c.fecha_emision as ultima_fecha
                    FROM erp_comprobantes_detalle cd
                    JOIN erp_comprobantes c ON cd.comprobante_id = c.id
                    JOIN erp_terceros t ON c.tercero_id = t.id
                    WHERE c.enterprise_id = %s AND cd.articulo_id = %s AND c.tipo_operacion = 'COMPRA'
                    ORDER BY c.fecha_emision DESC
                    LIMIT 1
                """
                await cursor.execute(sql, (enterprise_id, articulo_id))
                return await cursor.fetchone()

        return None

    @staticmethod
    async def sync_habitual_from_history(enterprise_id, articulo_id=None):
        """
        Analiza el historial de compras y marca como 'habitual' al proveedor 
        con mayor volumen de compra en los últimos 6 meses.
        """
        async with get_db_cursor() as cursor:
            # Si no se pasa articulo_id, podríamos iterar todos, pero por ahora manejamos uno
            if not articulo_id:
                return False

            sql_analysis = """
                SELECT c.tercero_id, COUNT(*) as ops_count, SUM(cd.cantidad) as total_qty
                FROM erp_comprobantes_detalle cd
                JOIN erp_comprobantes c ON cd.comprobante_id = c.id
                WHERE c.enterprise_id = %s AND cd.articulo_id = %s 
                  AND c.tipo_operacion = 'COMPRA'
                  AND c.fecha_emision > DATE_SUB(NOW(), INTERVAL 6 MONTH)
                GROUP BY c.tercero_id
                ORDER BY total_qty DESC
                LIMIT 1
            """
            await cursor.execute(sql_analysis, (enterprise_id, articulo_id))
            best = await cursor.fetchone()

            if best:
                prov_id = best[0]
                # Reset habituales para este articulo
                await cursor.execute("""
                    UPDATE cmp_articulos_proveedores 
                    SET es_habitual = 0 
                    WHERE enterprise_id = %s AND articulo_id = %s
                """, (enterprise_id, articulo_id))
                
                # Set nuevo habitual
                await cursor.execute("""
                    UPDATE cmp_articulos_proveedores 
                    SET es_habitual = 1 
                    WHERE enterprise_id = %s AND articulo_id = %s AND proveedor_id = %s
                """, (enterprise_id, articulo_id, prov_id))
                
                return True
        return False
