from database import get_db_cursor
from services.sourcing_service import SourcingService
from decimal import Decimal


class IndustrialCostingService:
    """
    Servicio para determinación de costos industriales (MSAC v4.0).
    Fase 1.2 + Fase 1.3: Roll-up recursivo de BOM + Gastos Indirectos (Overhead).
    
    Costo Industrial = Σ(Materiales x Cantidad x Factor Merma) + Σ(Gastos Indirectos Normalizados)
    """

    # -----------------------------------------------
    # FASE 1.2: COSTO DE MATERIALES (BOM Roll-up)
    # -----------------------------------------------
    @staticmethod
    def get_industrial_cost(enterprise_id, producto_id, recursive=False):
        """
        Calcula el costo total proyectado para un artículo producido.
        Si recursive=True, se usa para llamadas internas en explosión multi-nivel.
        Costo = Σ(Material_i * Qty_i * Costo_Capa_i) + Σ(Gastos Normalizados por Unidad)
        """
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Buscar receta activa
            cursor.execute("""
                SELECT id FROM cmp_recetas_bom 
                WHERE enterprise_id = %s AND producto_id = %s AND activo = 1
                LIMIT 1
            """, (enterprise_id, producto_id))
            rec_row = cursor.fetchone()
            
            if not rec_row:
                if recursive: return None
                return {
                    'articulo_id': producto_id,
                    'costo_materiales': 0.0,
                    'costo_indirectos': 0.0,
                    'costo_total_industrial': 0.0,
                    'message': 'Sin receta activa'
                }

            rec_id = rec_row['id']
            total_materiales = Decimal('0')

            # 2. Sumar materiales con merma
            cursor.execute("""
                SELECT articulo_id, cantidad, porcentaje_merma_esperada 
                FROM cmp_recetas_detalle WHERE receta_id = %s
            """, (rec_id,))
            items = cursor.fetchall()
                
            for it in items:
                # 2.1 Recursividad: ¿Es este componente también producido (semielaborado)?
                comp_costo_data = IndustrialCostingService.get_industrial_cost(
                    enterprise_id, it['articulo_id'], recursive=True
                )
                
                if comp_costo_data and comp_costo_data.get('costo_total_industrial', 0) > 0:
                    costo_unitario = Decimal(str(comp_costo_data['costo_total_industrial']))
                else:
                    # 2.2 Sourcing fallback: Última recepción → Mejor precio → 0
                    source = SourcingService.get_best_option(enterprise_id, it['articulo_id'], strategy='LAST_RECEPTION')
                    if not source:
                        source = SourcingService.get_best_option(enterprise_id, it['articulo_id'], strategy='BEST_PRICE')
                    costo_unitario = Decimal(str(source['precio_referencia'])) if source else Decimal('0')
                
                factor_merma = Decimal('1') + (Decimal(str(it['porcentaje_merma_esperada'])) / Decimal('100'))
                total_materiales += Decimal(str(it['cantidad'])) * factor_merma * costo_unitario

            # 3. FASE 1.3: Sumar Gastos Indirectos normalizados por unidad
            overhead = IndustrialCostingService.get_overhead_por_unidad(enterprise_id, producto_id, cursor)
            total_indirectos = Decimal(str(overhead['total_overhead_por_unidad']))
            margen = Decimal(str(overhead['margen_promedio']))

            total_costo_industrial = total_materiales + total_indirectos
            precio_sugerido = total_costo_industrial * (Decimal('1') + (margen / Decimal('100')))

            return {
                'articulo_id': producto_id,
                'costo_materiales': float(total_materiales),
                'costo_indirectos': float(total_indirectos),
                'costo_total_industrial': float(total_costo_industrial),
                'margen_aplicado': float(margen),
                'precio_sugerido': float(precio_sugerido),
                'detalle_overhead': overhead['detalle']
            }

    # -----------------------------------------------
    # FASE 1.3: GESTIÓN DE GASTOS INDIRECTOS
    # -----------------------------------------------
    @staticmethod
    def get_overhead_por_unidad(enterprise_id, articulo_id, cursor=None):
        """
        Recupera y normaliza los gastos indirectos de un artículo por unidad producida.
        Los gastos de tipo BATCH se dividen por cantidad_batch para obtener el costo/unidad.
        """
        def _query(cur):
            cur.execute("""
                SELECT tipo_gasto, descripcion, base_calculo, cantidad_batch, 
                       monto_estimado, porcentaje_margen_esperado
                FROM cmp_articulos_costos_indirectos
                WHERE enterprise_id = %s AND articulo_id = %s AND activo = 1
            """, (enterprise_id, articulo_id))
            return cur.fetchall()

        if cursor:
            rows = _query(cursor)
        else:
            with get_db_cursor(dictionary=True) as cur:
                rows = _query(cur)

        total_overhead = Decimal('0')
        margen_acum = Decimal('0')
        margen_count = 0
        detalle = []

        for row in rows:
            monto = Decimal(str(row['monto_estimado']))
            if row['base_calculo'] == 'BATCH' and row['cantidad_batch'] > 0:
                monto_por_unidad = monto / Decimal(str(row['cantidad_batch']))
            else:
                monto_por_unidad = monto

            total_overhead += monto_por_unidad
            if row['porcentaje_margen_esperado'] > 0:
                margen_acum += Decimal(str(row['porcentaje_margen_esperado']))
                margen_count += 1

            detalle.append({
                'tipo': row['tipo_gasto'],
                'descripcion': row['descripcion'],
                'monto_original': float(row['monto_estimado']),
                'base_calculo': row['base_calculo'],
                'cantidad_batch': row['cantidad_batch'],
                'costo_por_unidad': float(monto_por_unidad)
            })

        margen_promedio = (margen_acum / margen_count) if margen_count > 0 else Decimal('20')

        return {
            'total_overhead_por_unidad': float(total_overhead),
            'margen_promedio': float(margen_promedio),
            'detalle': detalle
        }

    @staticmethod
    def agregar_gasto_indirecto(enterprise_id, articulo_id, tipo_gasto, descripcion,
                                 monto_estimado, base_calculo='UNIDAD', cantidad_batch=1,
                                 margen=20.0, user_id=None):
        """
        Registra un nuevo componente de overhead para un artículo producido.
        """
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO cmp_articulos_costos_indirectos
                    (enterprise_id, articulo_id, tipo_gasto, descripcion, monto_estimado,
                     base_calculo, cantidad_batch, porcentaje_margen_esperado, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (enterprise_id, articulo_id, tipo_gasto, descripcion, monto_estimado,
                  base_calculo, cantidad_batch, margen, user_id))
            return cursor.lastrowid

    @staticmethod
    def eliminar_gasto_indirecto(enterprise_id, gasto_id, user_id=None):
        """Borrado lógico de un gasto indirecto."""
        with get_db_cursor() as cursor:
            cursor.execute("""
                UPDATE cmp_articulos_costos_indirectos
                SET activo = 0, user_id_update = %s
                WHERE id = %s AND enterprise_id = %s
            """, (user_id, gasto_id, enterprise_id))

    @staticmethod
    def listar_gastos(enterprise_id, articulo_id):
        """Devuelve todos los gastos activos de un artículo."""
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT id, tipo_gasto, descripcion, base_calculo, cantidad_batch,
                       monto_estimado, porcentaje_margen_esperado, created_at
                FROM cmp_articulos_costos_indirectos
                WHERE enterprise_id = %s AND articulo_id = %s AND activo = 1
                ORDER BY tipo_gasto, descripcion
            """, (enterprise_id, articulo_id))
            return cursor.fetchall()

    @staticmethod
    def aplicar_overhead_template(enterprise_id, template_id, articulo_id, user_id=None):
        """
        Aplica un template de overhead predefinido a un artículo.
        Útil para estandarizar costos en artículos similares.
        """
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT tipo_gasto, descripcion, monto_estimado, base_calculo, cantidad_batch
                FROM cmp_overhead_templates_detalle
                WHERE template_id = %s AND enterprise_id = %s
            """, (template_id, enterprise_id))
            items = cursor.fetchall()

            for it in items:
                cursor.execute("""
                    INSERT INTO cmp_articulos_costos_indirectos
                        (enterprise_id, articulo_id, tipo_gasto, descripcion, monto_estimado,
                         base_calculo, cantidad_batch, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (enterprise_id, articulo_id, it['tipo_gasto'], it['descripcion'],
                      it['monto_estimado'], it['base_calculo'], it['cantidad_batch'], user_id))

            return len(items)
