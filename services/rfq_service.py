
from database import get_db_cursor
from services.sourcing_service import SourcingService

class RfqService:
    """
    Servicio de Enriquecimiento de Solicitudes de Cotización (Phase 1.4).
    Asiste al comprador en la explosión de materiales y sugerencia de proveedores.
    """

    @staticmethod
    def explode_bom_for_rfq(enterprise_id, producto_id, cantidad_final=1):
        """
        Explosiona los materiales necesarios para producir 'cantidad_final' y 
        sugiere el proveedor habitual para cada insumo.
        """
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Buscar receta
            cursor.execute("""
                SELECT id FROM cmp_recetas_bom 
                WHERE enterprise_id = %s AND producto_id = %s AND activo = 1
                LIMIT 1
            """, (enterprise_id, producto_id))
            receta = cursor.fetchone()
            
            if not receta:
                return []

            # 2. Obtener detalles de la receta
            cursor.execute("""
                SELECT rd.articulo_id, rd.cantidad, rd.porcentaje_merma_esperada, a.nombre, a.codigo
                FROM cmp_recetas_detalle rd
                JOIN stk_articulos a ON rd.articulo_id = a.id
                WHERE rd.receta_id = %s
            """, (receta['id'],))
            items = cursor.fetchall()

            enriched_items = []
            for it in items:
                qty_needed = float(it['cantidad']) * cantidad_final
                merma = 1 + (float(it['porcentaje_merma_esperada']) / 100.0)
                final_qty = qty_needed * merma
                
                # Buscamos el proveedor habitual o el mejor precio
                best_source = SourcingService.get_best_option(enterprise_id, it['articulo_id'], strategy='HABITUAL')
                if not best_source:
                    best_source = SourcingService.get_best_option(enterprise_id, it['articulo_id'], strategy='BEST_PRICE')

                enriched_items.append({
                    'articulo_id': it['articulo_id'],
                    'codigo': it['codigo'],
                    'nombre': it['nombre'],
                    'cantidad_neta': qty_needed,
                    'cantidad_con_merma': final_qty,
                    'proveedor_sugerido_id': best_source['proveedor_id'] if best_source else None,
                    'proveedor_nombre': best_source['proveedor_nombre'] if best_source else 'SIN PROVEEDOR SUGERIDO',
                    'precio_referencia': float(best_source['precio_referencia']) if best_source else 0.0
                })

            return enriched_items

    @staticmethod
    def suggest_rfq_grouping(enterprise_id, items_to_quote):
        """
        Agrupa los materiales por proveedor sugerido para generar múltiples RFQs automáticas.
        """
        grouping = {}
        for it in items_to_quote:
            prov_id = it.get('proveedor_sugerido_id')
            if not prov_id:
                prov_id = 'PENDING'
            
            if prov_id not in grouping:
                grouping[prov_id] = {
                    'proveedor_nombre': it.get('proveedor_nombre'),
                    'items': []
                }
            grouping[prov_id]['items'].append(it)
        
        return grouping
