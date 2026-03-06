"""
ImportacionService
==================
Servicio de negocio para el Módulo de Importaciones — Etapa 2.

Responsabilidades:
  - Calcular el Costo Unitario de Importación (CUI) por artículo
  - Distribuir los cargos de importación (flete, seguro, derechos, etc.)
    proporcionalmente entre los ítems del embarque según su valor FOB
  - Registrar el ingreso al stock con el costo real CIF
  - Generar el asiento contable de importación (multimoneda)

Fórmulas:
  Valor CIF por artículo = FOB_unit + Flete_unit + Seguro_unit
  Derechos = % sobre CIF (varía por posición arancelaria, default 12%)
  Tasa Estadística = 3% sobre CIF
  CUI_ARS = (CIF_USD + otros_USD) * TC + (Derechos_ARS + Tasa_ARS + Otros_ARS + Despachante_ARS)
"""

import logging
from database import get_db_cursor

logger = logging.getLogger(__name__)


class ImportacionService:

    # ─────────────────────────────────────────────────────────────────────────
    # CARGOS
    # ─────────────────────────────────────────────────────────────────────────

    TIPOS_CARGO = [
        ("FLETE_INTERNACIONAL",   "Flete Internacional",              "USD"),
        ("SEGURO",                "Seguro de Transporte (% FOB)",     "USD"),
        ("DERECHOS_IMPORTACION",  "Derechos de Importación (% CIF)",  "ARS"),
        ("TASA_ESTADISTICA",      "Tasa Estadística (3% CIF)",        "ARS"),
        ("IVA_IMPORTACION",       "IVA Importación (% CIF+Derechos)", "ARS"),
        ("HONORARIOS_DESPACHANTE","Honorarios Despachante de Aduana", "ARS"),
        ("FLETE_INTERNO",         "Flete / Acarreo Interno",          "ARS"),
        ("ALMACENAMIENTO",        "Almacenamiento / Storage",         "ARS"),
        ("GASTOS_BANCARIOS",      "Gastos Bancarios / Transferencia", "USD"),
        ("OTRO",                  "Otro Cargo",                       "ARS"),
    ]

    @classmethod
    def get_cargos_orden(cls, orden_compra_id, enterprise_id):
        """Retorna todos los cargos registrados para una orden de importación."""
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT c.*, p.nombre as proveedor_nombre
                FROM imp_cargos c
                LEFT JOIN erp_terceros p ON c.proveedor_id = p.id
                WHERE c.orden_compra_id = %s AND c.enterprise_id = %s
                ORDER BY c.tipo_cargo, c.id
            """, (orden_compra_id, enterprise_id))
            return cursor.fetchall()

    @classmethod
    def agregar_cargo(cls, enterprise_id, orden_compra_id, tipo_cargo,
                      descripcion, monto_orig, moneda_orig, tipo_cambio,
                      proveedor_id=None, fecha=None, user_id=None, aplica_a_cui=1,
                      es_estimado=0, cargo_referencia_id=None):
        """
        Registra un cargo de importación y calcula su equivalente en ARS.
        """
        tipo_cambio = float(tipo_cambio or 0)
        monto_ars = float(monto_orig) * tipo_cambio if moneda_orig != 'ARS' else float(monto_orig)

        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO imp_cargos (
                    enterprise_id, orden_compra_id, tipo_cargo, descripcion,
                    proveedor_id, monto_orig, moneda_orig, tipo_cambio, monto_ars,
                    fecha, user_id, aplica_a_cui, estado, es_estimado, cargo_referencia_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'REGISTRADO', %s, %s)
            """, (
                enterprise_id, orden_compra_id, tipo_cargo, descripcion,
                proveedor_id, monto_orig, moneda_orig, tipo_cambio, monto_ars,
                fecha, user_id, aplica_a_cui, es_estimado, cargo_referencia_id
            ))
            return cursor.lastrowid

    # ─────────────────────────────────────────────────────────────────────────
    # ÍTEMS DE LA ORDEN
    # ─────────────────────────────────────────────────────────────────────────

    @classmethod
    def get_items_orden(cls, orden_compra_id, enterprise_id):
        """Retorna los ítems de la orden de compra con datos del artículo."""
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT i.*, i.cantidad_solicitada as cantidad, 
                       i.precio_unitario as precio_usd,
                       a.nombre as articulo_nombre, a.codigo as articulo_codigo,
                       a.costo as costo_actual
                FROM cmp_detalles_orden i
                JOIN stk_articulos a ON i.articulo_id = a.id
                WHERE i.orden_id = %s AND i.enterprise_id = %s
            """, (orden_compra_id, enterprise_id))
            return cursor.fetchall()

    # ─────────────────────────────────────────────────────────────────────────
    # CÁLCULO DEL CUI (Costo Unitario de Importación)
    # ─────────────────────────────────────────────────────────────────────────

    @classmethod
    def calcular_cui(cls, orden_compra_id, enterprise_id, tipo_cambio_usd=None):
        """
        Calcula el CUI (Costo Unitario de Importación) para cada artículo
        de la orden, distribuyendo los cargos proporcionalmente a su valor FOB.

        Algoritmo:
          1. Obtener ítems de la OC con su cantidad y precio unitario (en USD)
          2. Calcular valor FOB total del embarque
          3. Pro-ratear los cargos entre artículos según su peso relativo (FOB)
          4. CUI por artículo = (FOB_unit + cargos_prorrateados_unit) en ARS

        Retorna: lista de dicts con el CUI calculado por artículo.
        """
        if not tipo_cambio_usd:
            from services.bcra_service import CurrencyRateService
            tipo_cambio_usd = CurrencyRateService.get_tipo_cambio('USD', 'OFICIAL_VENDEDOR') or 1000.0

        tc = float(tipo_cambio_usd)

        # 1. Ítems de la OC
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT i.articulo_id, i.cantidad_solicitada as cantidad,
                       COALESCE(i.precio_unitario, 0) as precio_usd,
                       a.nombre as articulo_nombre, a.codigo
                FROM cmp_detalles_orden i
                JOIN stk_articulos a ON i.articulo_id = a.id
                WHERE i.orden_id = %s AND i.enterprise_id = %s
            """, (orden_compra_id, enterprise_id))
            items = cursor.fetchall()

        if not items:
            # Intentar con tabla alternativa de items
            with get_db_cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT i.articulo_id, i.cantidad,
                           COALESCE(i.precio_cotizado, 0) as precio_usd,
                           a.nombre as articulo_nombre, a.codigo
                    FROM cmp_items_cotizacion i
                    JOIN cmp_cotizaciones cot ON i.cotizacion_id = cot.id
                    JOIN stk_articulos a ON i.articulo_id = a.id
                    WHERE cot.id = (
                        SELECT cotizacion_id FROM cmp_ordenes_compra WHERE id = %s
                    ) AND i.enterprise_id = %s
                """, (orden_compra_id, enterprise_id))
                items = cursor.fetchall()

        # 2. Cargos que deben incluirse en CUI
        cargos = cls.get_cargos_orden(orden_compra_id, enterprise_id)
        cargos = [c for c in cargos if c.get('aplica_a_cui', 1)]

        # Cargos en USD y en ARS separados
        total_cargos_usd = sum(
            float(c['monto_orig'] or 0) for c in cargos if c['moneda_orig'] != 'ARS'
        )
        total_cargos_ars = sum(
            float(c['monto_ars'] or 0) for c in cargos if c['moneda_orig'] == 'ARS'
        )

        # 2b. Integrar Tributos del Despacho (Derechos, Tasa Estadística, etc.)
        total_tributos_ars = 0
        despacho = cls.get_despacho(orden_compra_id, enterprise_id)
        if despacho:
            # Estos tributos ya están en ARS en la tabla imp_despachos
            total_tributos_ars += float(despacho.get('derechos_ars', 0) or 0)
            total_tributos_ars += float(despacho.get('tasa_estadistica_ars', 0) or 0)
            total_tributos_ars += float(despacho.get('otros_tributos_ars', 0) or 0)
            
        total_cargos_ars += total_tributos_ars

        # 3. Valor FOB total del embarque
        total_fob_usd = sum(
            float(i['cantidad'] or 0) * float(i['precio_usd'] or 0) for i in items
        )

        if total_fob_usd <= 0:
            logger.warning(f"[CUI] Orden {orden_compra_id}: FOB total = 0, no se puede calcular CUI")
            return {'success': False, 'message': 'El valor FOB total de la orden es 0. Verifique los precios de los ítems.', 'items': []}

        resultados = []
        for item in items:
            qty    = float(item['cantidad'] or 1)
            p_usd  = float(item['precio_usd'] or 0)
            fob_item_usd = qty * p_usd

            # Peso relativo del ítem en el embarque
            peso = (fob_item_usd / total_fob_usd) if total_fob_usd > 0 else (1 / len(items))

            # Cargos prorrateados
            cargos_usd_item  = total_cargos_usd * peso
            cargos_ars_item  = total_cargos_ars * peso

            # CUI en ARS por unidad
            fob_ars_unit        = p_usd * tc
            cargos_usd_ars_unit = (cargos_usd_item / qty) * tc if qty > 0 else 0
            cargos_ars_unit     = (cargos_ars_item / qty) if qty > 0 else 0

            cui_por_unidad = fob_ars_unit + cargos_usd_ars_unit + cargos_ars_unit

            resultados.append({
                'articulo_id':       item['articulo_id'],
                'articulo_nombre':   item['articulo_nombre'],
                'articulo_codigo':   item['codigo'],
                'cantidad':          qty,
                'precio_fob_usd':    p_usd,
                'fob_total_usd':     fob_item_usd,
                'peso_relativo_pct': round(peso * 100, 2),
                'cargos_usd_unitario': round(cargos_usd_item / qty, 4) if qty else 0,
                'cargos_ars_unitario': round(cargos_ars_item / qty, 4) if qty else 0,
                'cui_ars':            round(cui_por_unidad, 4),
                'cui_total_ars':      round(cui_por_unidad * qty, 2),
                'tipo_cambio_usd':    tc,
            })

        return {
            'items':               resultados,
            'total_fob_usd':       round(total_fob_usd, 2),
            'total_cargos_usd':    round(total_cargos_usd, 2),
            'total_cargos_ars':    round(total_cargos_ars, 2),
            'total_tributos_ars':  round(total_tributos_ars, 2),
            'total_operativos_ars': round(total_cargos_ars - total_tributos_ars, 2),
            'tipo_cambio':         tc,
            'total_cui_ars':       round(sum(r['cui_total_ars'] for r in resultados), 2),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # INGRESO AL STOCK
    # ─────────────────────────────────────────────────────────────────────────

    @classmethod
    def registrar_ingreso_stock(cls, orden_compra_id, despacho_id,
                                enterprise_id, deposito_id, user_id,
                                tipo_cambio_usd=None):
        """
        Registra el ingreso al stock de los artículos de una importación.
        Pasos:
          1. Calcular CUI
          2. Crear movimiento de stock tipo ENTRADA (motivo = 'Compra / Recepción')
          3. Crear detalle de movimiento por artículo
          4. Actualizar existencias en stk_existencias
          5. Actualizar costo en stk_articulos (costo promedio ponderado)
          6. Actualizar estado del despacho y de la OC
          7. Registrar snapshot de CUI en imp_despachos_items
          Retorna: dict con resultado del ingreso.
        """
        cui_resultado = cls.calcular_cui(orden_compra_id, enterprise_id, tipo_cambio_usd)
        items_cui = cui_resultado['items']

        if not items_cui:
            return {'success': False, 'message': 'No hay ítems con CUI calculado para ingresar.'}

        with get_db_cursor(dictionary=True) as cursor:
            # a) Obtener motivo de compra
            cursor.execute("""
                SELECT id FROM stk_motivos
                WHERE (enterprise_id = %s OR enterprise_id = 0)
                  AND automatico = 1 AND tipo = 'ENTRADA'
                ORDER BY enterprise_id DESC LIMIT 1
            """, (enterprise_id,))
            motivo_row = cursor.fetchone()
            if not motivo_row:
                return {'success': False, 'message': 'No existe motivo de ENTRADA automático en stk_motivos.'}
            motivo_id = motivo_row['id']

            # b) Crear cabecera de movimiento
            from datetime import date
            cursor.execute("""
                INSERT INTO stk_movimientos (
                    enterprise_id, fecha, motivo_id, deposito_destino_id,
                    comprobante_id, user_id, observaciones, estado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'CONFIRMADO')
            """, (
                enterprise_id, date.today(), motivo_id, deposito_id,
                orden_compra_id, user_id,
                f"IMPORTACIÓN OC#{orden_compra_id} Desp.#{despacho_id}"
            ))
            movimiento_id = cursor.lastrowid

            artículos_ingresados = []

            for item in items_cui:
                art_id  = item['articulo_id']
                qty     = int(item['cantidad'])
                cui     = float(item['cui_ars'])

                # c) Detalle de movimiento
                cursor.execute("""
                    INSERT INTO stk_movimientos_detalle (movimiento_id, articulo_id, cantidad)
                    VALUES (%s, %s, %s)
                """, (movimiento_id, art_id, qty))

                # d) Actualizar existencias (UPSERT)
                cursor.execute("""
                    INSERT INTO stk_existencias (enterprise_id, deposito_id, articulo_id, cantidad)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE cantidad = cantidad + VALUES(cantidad)
                """, (enterprise_id, deposito_id, art_id, qty))

                # [MOD SOD/CISA] - El costo no se actualiza directamente. 
                # Pasa a la bandeja de Pricing para aprobación final.
                # CPP se calculará al aprobar en Pricing.
                
                # Guardamos solo el snapshot en el despacho (Paso f)
                pass

                # f) Snapshot en imp_despachos_items
                cursor.execute("""
                    INSERT INTO imp_despachos_items (
                        despacho_id, orden_compra_id, articulo_id,
                        cantidad, precio_unitario_usd, valor_total_usd,
                        costo_derechos_ars, costo_otros_ars, cui_ars
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE cui_ars = VALUES(cui_ars)
                """, (
                    despacho_id, orden_compra_id, art_id,
                    qty,
                    float(item['precio_fob_usd']),
                    float(item['fob_total_usd']),
                    float(item['cargos_ars_unitario'] * qty),
                    0,
                    cui
                ))

                artículos_ingresados.append({
                    'articulo_id': art_id,
                    'nombre': item['articulo_nombre'],
                    'cantidad': qty,
                    'cui_ars': cui
                })

            # g) Actualizar estado despacho y OC
            cursor.execute("""
                UPDATE imp_despachos SET estado = 'INGRESADO', fecha_liberacion = CURDATE()
                WHERE id = %s AND enterprise_id = %s
            """, (despacho_id, enterprise_id))

            cursor.execute("""
                UPDATE cmp_ordenes_compra
                SET estado_importacion = 'INGRESADO', estado = 'RECIBIDA_TOTAL'
                WHERE id = %s AND enterprise_id = %s
            """, (orden_compra_id, enterprise_id))

            # h) Generar Asiento Contable - [POSTERGADO]
            # Ahora el asiento se genera cuando el Gerente de Costos aprueba el Pricing.
            asiento_id = None
            
            # i) Inyectar en Pricing (NUEVO)
            from pricing.service import PricingService
            pricing_items = [
                {'articulo_id': r['articulo_id'], 'costo_calculado': r['cui_ars']} 
                for r in artículos_ingresados
            ]
            PricingService.generar_propuestas_desde_costo(
                enterprise_id=enterprise_id,
                origen='IMPORTACION',
                documento_origen_id=despacho_id,
                items_data=pricing_items,
                user_id=user_id
            )

        logger.info(f"[ImportacionService] Ingreso OC#{orden_compra_id}: {len(artículos_ingresados)} artículos. Asiento: {asiento_id}")

        return {
            'success':              True,
            'movimiento_id':        movimiento_id,
            'asiento_id':           asiento_id,
            'articulos_ingresados': artículos_ingresados,
            'total_cui_ars':        cui_resultado['total_cui_ars'],
            'tipo_cambio':          cui_resultado['tipo_cambio'],
        }

    # ─────────────────────────────────────────────────────────────────────────
    # DESPACHO ADUANERO
    # ─────────────────────────────────────────────────────────────────────────

    @classmethod
    def get_despacho(cls, orden_compra_id, enterprise_id):
        """Retorna el despacho activo de una orden (puede ser None)."""
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT d.*, p.nombre as despachante_nombre
                FROM imp_despachos d
                LEFT JOIN erp_terceros p ON d.despachante_id = p.id
                WHERE d.orden_compra_id = %s AND d.enterprise_id = %s
                ORDER BY d.id DESC LIMIT 1
            """, (orden_compra_id, enterprise_id))
            return cursor.fetchone()

    @classmethod
    def crear_o_actualizar_despacho(cls, enterprise_id, orden_compra_id, data, user_id):
        """
        Crea o actualiza el despacho aduanero de una orden de importación.
        `data` es un dict con los campos del despacho.
        Retorna el id del despacho.
        """
        despacho_existente = cls.get_despacho(orden_compra_id, enterprise_id)

        campos = {
            'numero_despacho':              data.get('numero_despacho'),
            'despachante_id':               data.get('despachante_id') or None,
            'fecha_oficializacion':         data.get('fecha_oficializacion') or None,
            'canal':                        data.get('canal', 'VERDE').upper(),
            'estado':                       data.get('estado', 'PENDIENTE').upper(),
            'valor_fob_usd':                float(data.get('valor_fob_usd', 0) or 0),
            'valor_cif_usd':                float(data.get('valor_cif_usd', 0) or 0),
            'derechos_ars':                 float(data.get('derechos_ars', 0) or 0),
            'tasa_estadistica_ars':         float(data.get('tasa_estadistica_ars', 0) or 0),
            'otros_tributos_ars':           float(data.get('otros_tributos_ars', 0) or 0),
            'tipo_cambio_oficializacion':   float(data.get('tipo_cambio_oficializacion', 0) or 0) or None,
            'observaciones':                data.get('observaciones', ''),
            # Etapa 4: Logística
            'transportista':                data.get('transportista'),
            'guia_bl_tracking':             data.get('guia_bl_tracking'),
            'fecha_embarque':               data.get('fecha_embarque') or None,
            'fecha_arribo_estimada':        data.get('fecha_arribo_estimada') or None,
            'fecha_arribo_real':            data.get('fecha_arribo_real') or None,
            'puerto_embarque':              data.get('puerto_embarque'),
            'puerto_destino':               data.get('puerto_destino'),
            'peso_kg':                      float(data.get('peso_kg', 0) or 0) or None,
            # Etapa 5: Puerto y Demoras
            'dias_libres_puerto':           int(data.get('dias_libres_puerto', 0) or 0),
            'costo_demora_diario_usd':      float(data.get('costo_demora_diario_usd', 0) or 0),
            'fecha_devolucion_contenedor':  data.get('fecha_devolucion_contenedor') or None,
        }

        with get_db_cursor() as cursor:
            if despacho_existente:
                cursor.execute("""
                    UPDATE imp_despachos
                    SET numero_despacho = %s, despachante_id = %s,
                        fecha_oficializacion = %s, canal = %s, estado = %s,
                        valor_fob_usd = %s, valor_cif_usd = %s,
                        derechos_ars = %s, tasa_estadistica_ars = %s,
                        otros_tributos_ars = %s, tipo_cambio_oficializacion = %s,
                        observaciones = %s,
                        transportista = %s, guia_bl_tracking = %s, fecha_embarque = %s,
                        fecha_arribo_estimada = %s, fecha_arribo_real = %s,
                        puerto_embarque = %s, puerto_destino = %s, bultos = %s, peso_kg = %s,
                        dias_libres_puerto = %s, costo_demora_diario_usd = %s, fecha_devolucion_contenedor = %s
                    WHERE id = %s AND enterprise_id = %s
                """, (
                    campos['numero_despacho'], campos['despachante_id'],
                    campos['fecha_oficializacion'], campos['canal'], campos['estado'],
                    campos['valor_fob_usd'], campos['valor_cif_usd'],
                    campos['derechos_ars'], campos['tasa_estadistica_ars'],
                    campos['otros_tributos_ars'], campos['tipo_cambio_oficializacion'],
                    campos['observaciones'],
                    campos['transportista'], campos['guia_bl_tracking'], campos['fecha_embarque'],
                    campos['fecha_arribo_estimada'], campos['fecha_arribo_real'],
                    campos['puerto_embarque'], campos['puerto_destino'], campos['bultos'], campos['peso_kg'],
                    campos['dias_libres_puerto'], campos['costo_demora_diario_usd'], campos['fecha_devolucion_contenedor'],
                    despacho_existente['id'], enterprise_id
                ))
                despacho_id = despacho_existente['id']
            else:
                cursor.execute("""
                    INSERT INTO imp_despachos (
                        enterprise_id, orden_compra_id,
                        numero_despacho, despachante_id, fecha_oficializacion,
                        canal, estado, valor_fob_usd, valor_cif_usd,
                        derechos_ars, tasa_estadistica_ars, otros_tributos_ars,
                        tipo_cambio_oficializacion, observaciones, user_id,
                        transportista, guia_bl_tracking, fecha_embarque,
                        fecha_arribo_estimada, fecha_arribo_real,
                        puerto_embarque, puerto_destino, bultos, peso_kg,
                        dias_libres_puerto, costo_demora_diario_usd, fecha_devolucion_contenedor
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    enterprise_id, orden_compra_id,
                    campos['numero_despacho'], campos['despachante_id'],
                    campos['fecha_oficializacion'], campos['canal'], campos['estado'],
                    campos['valor_fob_usd'], campos['valor_cif_usd'],
                    campos['derechos_ars'], campos['tasa_estadistica_ars'],
                    campos['otros_tributos_ars'], campos['tipo_cambio_oficializacion'],
                    campos['observaciones'], user_id,
                    campos['transportista'], campos['guia_bl_tracking'], campos['fecha_embarque'],
                    campos['fecha_arribo_estimada'], campos['fecha_arribo_real'],
                    campos['puerto_embarque'], campos['puerto_destino'], campos['bultos'], campos['peso_kg'],
                    campos['dias_libres_puerto'], campos['costo_demora_diario_usd'], campos['fecha_devolucion_contenedor']
                ))
                despacho_id = cursor.lastrowid

            # Actualizar estado de la OC
            cursor.execute("""
                UPDATE cmp_ordenes_compra SET estado_importacion = %s
                WHERE id = %s AND enterprise_id = %s
            """, (campos['estado'], orden_compra_id, enterprise_id))

        return despacho_id

    @classmethod
    def get_logistica_stats(cls, despacho):
        """Calcula estadísticas y lead times de logística."""
        if not despacho or not despacho.get('fecha_embarque'):
            return None
        
        from datetime import date
        etd = despacho['fecha_embarque']
        eta = despacho['fecha_arribo_estimada']
        ata = despacho['fecha_arribo_real']
        today = date.today()

        transit_time_est = (eta - etd).days if eta and etd else 0
        transit_time_real = (ata - etd).days if ata and etd else ((today - etd).days if etd else 0)
        
        # Cálculo de Demoras (Demurrage)
        dias_demora = 0
        costo_demora = 0
        if ata:
            fecha_fin = despacho.get('fecha_devolucion_contenedor') or today
            dias_en_puerto = (fecha_fin - ata).days
            dias_libres = despacho.get('dias_libres_puerto', 0) or 0
            dias_demora = max(0, dias_en_puerto - dias_libres)
            costo_demora = dias_demora * float(despacho.get('costo_demora_diario_usd', 0) or 0)

        # Estado simplificado
        if ata:
            estado_log = "EN DESTINO"
        elif eta and today > eta:
            estado_log = "DEMORADO"
        elif etd and today >= etd:
            estado_log = "EN TRANSITO"
        else:
            estado_log = "PENDIENTE EMBARQUE"

        return {
            'transit_time_est': transit_time_est,
            'transit_time_real': transit_time_real,
            'estado_logistico': estado_log,
            'dias_para_arribo': (eta - today).days if eta and not ata else 0,
            'dias_demora': dias_demora,
            'costo_demora_usd': costo_demora
        }

    @classmethod
    def get_dashboard_stats(cls, enterprise_id):
        """Obtiene estadísticas globales para el Dashboard Ejecutivo de Importaciones."""
        from datetime import date, timedelta
        hoy = date.today()
        proximos_15 = hoy + timedelta(days=15)
        proximos_30 = hoy + timedelta(days=30)

        stats = {
            'resumen': {
                'total_ocs': 0,
                'en_tramite': 0,
                'embarcadas': 0,
                'en_aduana': 0,
                'finalizadas': 0
            },
            'logistica': {
                'bultos_en_transito': 0,
                'peso_en_transito_kg': 0,
                'proximos_arribos_15d': 0,
                'proximos_arribos_30d': 0
            },
            'financiero': {
                'total_fob_usd': 0,
                'total_pagado_usd': 0,
                'pendiente_pago_usd': 0
            },
            'alertas': {
                'demoras_ais': 0,
                'detalles': []
            },
            'ordenes_recientes': []
        }

        with get_db_cursor(dictionary=True) as cursor:
            # 1. Resumen por Estado de Importación
            cursor.execute("""
                SELECT estado_importacion, COUNT(*) as cant
                FROM cmp_ordenes_compra
                WHERE enterprise_id = %s AND proveedor_id IN (SELECT id FROM erp_terceros WHERE pais_origen != 'AR')
                GROUP BY estado_importacion
            """, (enterprise_id,))
            for row in cursor.fetchall():
                est = row['estado_importacion'] or 'PENDIENTE'
                stats['resumen']['total_ocs'] += row['cant']
                if est in ('PENDIENTE', 'PAGADO'): stats['resumen']['en_tramite'] += row['cant']
                elif est == 'EMBARCADO': stats['resumen']['embarcadas'] += row['cant']
                elif est in ('PRESENTADO', 'EN_REVISION', 'OBSERVADO', 'LIBERADO'): stats['resumen']['en_aduana'] += row['cant']
                elif est == 'INGRESADO': stats['resumen']['finalizadas'] += row['cant']

            # 2. Logística y Fechas
            cursor.execute("""
                SELECT d.*, o.numero_comprobante
                FROM imp_despachos d
                JOIN cmp_ordenes_compra o ON d.orden_compra_id = o.id
                WHERE d.enterprise_id = %s AND d.estado != 'INGRESADO'
            """, (enterprise_id,))
            for d in cursor.fetchall():
                if d['fecha_embarque'] and not d['fecha_arribo_real']:
                    stats['logistica']['bultos_en_transito'] += (d['bultos'] or 0)
                    stats['logistica']['peso_en_transito_kg'] += float(d['peso_kg'] or 0)
                
                eta = d['fecha_arribo_estimada']
                if eta and not d['fecha_arribo_real']:
                    if hoy <= eta <= proximos_15: stats['logistica']['proximos_arribos_15d'] += 1
                    if hoy <= eta <= proximos_30: stats['logistica']['proximos_arribos_30d'] += 1

            # 3. Financiero
            # Total FOB: Suma de monto_total de las OCs de importación activas (moneda USD)
            cursor.execute("""
                SELECT SUM(monto_total) as total 
                FROM cmp_ordenes_compra 
                WHERE enterprise_id = %s AND es_importacion = 1 AND moneda = 'USD'
                  AND estado_importacion != 'INGRESADO'
            """, (enterprise_id,))
            stats['financiero']['total_fob_usd'] = float(cursor.fetchone()['total'] or 0)

            # Total Pagado: Suma de pagos en USD
            cursor.execute("""
                SELECT SUM(monto_orig) as total FROM imp_pagos
                WHERE enterprise_id = %s AND moneda = 'USD'
            """, (enterprise_id,))
            stats['financiero']['total_pagado_usd'] = float(cursor.fetchone()['total'] or 0)
            
            # Pendiente (estimado)
            stats['financiero']['pendiente_pago_usd'] = max(0, stats['financiero']['total_fob_usd'] - stats['financiero']['total_pagado_usd'])

            # 3.5 Alertas de Desvío AIS (Stage 5.3)
            cursor.execute("""
                SELECT v.orden_compra_id, v.vessel_name, v.eta_predicted, d.fecha_arribo_estimada, o.numero_comprobante
                FROM imp_vessel_tracking v
                JOIN imp_despachos d ON v.orden_compra_id = d.orden_compra_id AND v.enterprise_id = d.enterprise_id
                JOIN cmp_ordenes_compra o ON d.orden_compra_id = o.id
                WHERE v.enterprise_id = %s 
                  AND v.id IN (SELECT MAX(id) FROM imp_vessel_tracking WHERE enterprise_id = %s GROUP BY orden_compra_id)
                  AND d.estado != 'INGRESADO'
                  AND v.eta_predicted IS NOT NULL
                  AND d.fecha_arribo_estimada IS NOT NULL
            """, (enterprise_id, enterprise_id))
            
            for al in cursor.fetchall():
                eta_ais = al['eta_predicted'].date() if hasattr(al['eta_predicted'], 'date') else al['eta_predicted']
                eta_man = al['fecha_arribo_estimada']
                
                if eta_ais and eta_man:
                    diff = (eta_ais - eta_man).days
                    if diff > 1:
                        stats['alertas']['demoras_ais'] += 1
                        stats['alertas']['detalles'].append({
                            'orden_id': al['orden_compra_id'],
                            'comprobante': al['numero_comprobante'],
                            'buque': al['vessel_name'],
                            'dias_desvio': diff,
                            'eta_ais': eta_ais.strftime('%d/%m/%Y') if hasattr(eta_ais, 'strftime') else str(eta_ais)
                        })


            # 4. Cronograma de Eventos (Próximos 90 días)
            eventos = []
            
            # a) Arribos (ETAs)
            cursor.execute("""
                SELECT d.fecha_arribo_estimada as fecha, o.numero_comprobante, t.nombre as proveedor, 'ARRIBO' as tipo
                FROM imp_despachos d
                JOIN cmp_ordenes_compra o ON d.orden_compra_id = o.id
                JOIN erp_terceros t ON o.proveedor_id = t.id
                WHERE d.enterprise_id = %s AND d.fecha_arribo_estimada >= %s
                  AND d.fecha_arribo_real IS NULL
                ORDER BY d.fecha_arribo_estimada
            """, (enterprise_id, hoy))
            for res in cursor.fetchall():
                eventos.append({
                    'fecha': res['fecha'].isoformat(),
                    'tipo': 'ARRIBO',
                    'titulo': f"Arribo OC {res['numero_comprobante']}",
                    'subtitulo': res['proveedor'],
                    'color': '#38bdf8'
                })

            # b) Pagos Pendientes (Estimados por fecha_emision + 30 o 60 días si no hay fecha_pago_est)
            # Nota: Aquí usamos una lógica simplificada para el dashboard
            cursor.execute("""
                SELECT o.fecha_emision, o.monto_total, o.numero_comprobante, t.nombre as proveedor
                FROM cmp_ordenes_compra o
                JOIN erp_terceros t ON o.proveedor_id = t.id
                WHERE o.enterprise_id = %s AND o.es_importacion = 1 AND o.moneda = 'USD'
                  AND o.estado_importacion NOT IN ('INGRESADO', 'CANCELADO')
            """, (enterprise_id,))
            for res in cursor.fetchall():
                # Simulamos vencimiento a 30 días de la emisión como placeholder si no hay otro dato
                venc = res['fecha_emision'] + datetime.timedelta(days=30)
                if venc >= hoy:
                    eventos.append({
                        'fecha': venc.isoformat(),
                        'tipo': 'PAGO',
                        'titulo': f"Pago Est. OC {res['numero_comprobante']}",
                        'subtitulo': f"{res['proveedor']} (U$S {float(res['monto_total'] or 0):,.0f})",
                        'color': '#fbbf24'
                    })

            stats['calendario'] = sorted(eventos, key=lambda x: x['fecha'])[:15]

            # 5. Órdenes con actividad reciente

            for o in stats['ordenes_recientes']:
                if o['fecha_arribo_estimada']:
                    o['fecha_arribo_estimada'] = o['fecha_arribo_estimada'].isoformat()

        return stats

    @classmethod
    def get_desvio_costos(cls, orden_id, enterprise_id):
        """Analiza desviaciones entre costos estimados y reales."""
        cargos = cls.get_cargos_orden(orden_id, enterprise_id)
        
        resumen = {} # {tipo_cargo: {estimado: X, real: Y, delta: Z}}
        
        for c in cargos:
            tipo = c['tipo_cargo']
            if tipo not in resumen:
                resumen[tipo] = {
                    'label': next((t[1] for t in cls.TIPOS_CARGO if t[0] == tipo), tipo),
                    'estimado_ars': 0, 
                    'real_ars': 0, 
                    'moneda': c['moneda_orig'],
                    'estimado_orig': 0,
                    'real_orig': 0
                }
            
            if c['es_estimado']:
                resumen[tipo]['estimado_ars'] += float(c['monto_ars'] or 0)
                resumen[tipo]['estimado_orig'] += float(c['monto_orig'] or 0)
            else:
                resumen[tipo]['real_ars'] += float(c['monto_ars'] or 0)
                resumen[tipo]['real_orig'] += float(c['monto_orig'] or 0)

        # Calcular deltas
        total_estimado = 0
        total_real = 0
        
        for k, v in resumen.items():
            v['desvio_ars'] = v['real_ars'] - v['estimado_ars']
            v['desvio_pct'] = (v['desvio_ars'] / v['estimado_ars'] * 100) if v['estimado_ars'] > 0 else 0
            total_estimado += v['estimado_ars']
            total_real += v['real_ars']

        return {
            'por_categoria': resumen,
            'total_estimado_ars': total_estimado,
            'total_real_ars': total_real,
            'desvio_total_ars': total_real - total_estimado,
            'desvio_total_pct': (total_real - total_estimado) / total_estimado * 100 if total_estimado > 0 else 0
        }




    @classmethod
    def _generar_asiento_importacion(cls, cursor, orden_id, enterprise_id, total_ars, user_id=None):
        """Genera el asiento contable por el ingreso de la importación."""
        # 1. Obtener cuentas (Mercaderías 1.4.01, Importaciones en Curso 1.4.05 o Proveedores 2.1.01)
        cursor.execute("""
            SELECT id, codigo FROM cont_plan_cuentas 
            WHERE (enterprise_id = %s OR enterprise_id = 0) AND codigo IN ('1.4.01', '1.4.05', '2.1.01')
        """, (enterprise_id,))
        cuentas = {r['codigo']: r['id'] for r in cursor.fetchall()}
        
        if '1.4.01' not in cuentas:
            logger.warning("[Contabilidad] Falta cuenta 1.4.01. No se genera asiento.")
            return None
        
        # Preferir 1.4.05 (Importaciones en curso) sobre 2.1.01 (Proveedores)
        cuenta_haber = cuentas.get('1.4.05') or cuentas.get('2.1.01')
        if not cuenta_haber:
            logger.warning("[Contabilidad] Faltan cuentas 1.4.05 o 2.1.01. No se genera asiento.")
            return None

        # 2. Próximo nro asiento
        cursor.execute("SELECT COALESCE(MAX(numero_asiento), 0) + 1 as proximo FROM cont_asientos WHERE enterprise_id = %s", (enterprise_id,))
        proximo_nro = cursor.fetchone()['proximo']

        # 3. Cabecera
        concepto = f"Ingreso Importación OC#{orden_id}"
        cursor.execute("""
            INSERT INTO cont_asientos (enterprise_id, fecha, concepto, modulo_origen, comprobante_id, numero_asiento, user_id, estado)
            VALUES (%s, CURDATE(), %s, 'COMPRAS', %s, %s, %s, 'CONFIRMADO')
        """, (enterprise_id, concepto, orden_id, proximo_nro, user_id))
        asiento_id = cursor.lastrowid

        # 4. Detalles
        # DEBE: Mercaderías (Activo aumenta)
        cursor.execute("""
            INSERT INTO cont_asientos_detalle (asiento_id, enterprise_id, cuenta_id, debe, haber, glosa)
            VALUES (%s, %s, %s, %s, 0, %s)
        """, (asiento_id, enterprise_id, cuentas['1.4.01'], total_ars, f"CUI Importación OC#{orden_id}"))

        # HABER: Importaciones en Curso / Proveedores (Pasivo/Regularizadora aumenta)
        cursor.execute("""
            INSERT INTO cont_asientos_detalle (asiento_id, enterprise_id, cuenta_id, debe, haber, glosa)
            VALUES (%s, %s, %s, 0, %s, %s)
        """, (asiento_id, enterprise_id, cuenta_haber, total_ars, f"Provision Importación OC#{orden_id}"))

        return asiento_id
    # ─────────────────────────────────────────────────────────────────────────
    # PAGOS INTERNACIONALES
    # ─────────────────────────────────────────────────────────────────────────

    @classmethod
    def get_pagos_orden(cls, orden_id, enterprise_id):
        """Retorna los pagos realizados para una orden de importación."""
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT p.*, b.nombre as banco_nombre
                FROM imp_pagos p
                LEFT JOIN fin_bancos b ON p.banco_id = b.id
                WHERE p.orden_compra_id = %s AND p.enterprise_id = %s
                ORDER BY p.fecha DESC
            """, (orden_id, enterprise_id))
            return cursor.fetchall()

    @classmethod
    def agregar_pago(cls, enterprise_id, orden_id, proveedor_id, monto_orig, moneda, 
                     tipo_cambio, banco_id, fecha, swift, observaciones, user_id):
        """
        Registra un pago internacional (Transferencia Bancaria / SWIFT).
        Genera el asiento contable correspondiente.
        """
        tipo_cambio = float(tipo_cambio)
        monto_ars = float(monto_orig) * tipo_cambio
        
        with get_db_cursor() as cursor:
            # 1. Insertar pago
            cursor.execute("""
                INSERT INTO imp_pagos (
                    enterprise_id, orden_compra_id, proveedor_id, 
                    fecha, moneda, monto_orig, tipo_cambio, monto_ars,
                    banco_id, referencia_swift, estado, observaciones, user_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'CONFIRMADO', %s, %s)
            """, (
                enterprise_id, orden_id, proveedor_id,
                fecha, moneda, monto_orig, tipo_cambio, monto_ars,
                banco_id, swift, observaciones, user_id
            ))
            pago_id = cursor.lastrowid

            # 2. Generar Asiento Contable
            try:
                asiento_id = cls._generar_asiento_pago_internacional(
                    cursor, enterprise_id, pago_id, orden_id, proveedor_id,
                    moneda, monto_orig, tipo_cambio, monto_ars, banco_id, user_id
                )
                if asiento_id:
                    cursor.execute("UPDATE imp_pagos SET asiento_id = %s WHERE id = %s", (asiento_id, pago_id))
            except Exception as e:
                logger.error(f"[Finanzas] Error al generar asiento de pago: {e}")
                asiento_id = None

        return {'success': True, 'pago_id': pago_id, 'asiento_id': asiento_id}

    @classmethod
    def _generar_asiento_pago_internacional(cls, cursor, enterprise_id, pago_id, orden_id, 
                                            proveedor_id, moneda, monto_orig, tc, monto_ars, 
                                            banco_id, user_id):
        """Genera el asiento: Proveedores (Debe) a Banco (Haber)."""
        # a) Obtener Cuenta del Banco
        cursor.execute("SELECT cuenta_contable_id FROM fin_bancos WHERE id = %s", (banco_id,))
        row_b = cursor.fetchone()
        cuenta_banco_id = row_b[0] if row_b else None
        
        # b) Obtener Cuenta Proveedores (2.1.01)
        cursor.execute("""
            SELECT id FROM cont_plan_cuentas 
            WHERE (enterprise_id = %s OR enterprise_id = 0) AND codigo = '2.1.01'
        """, (enterprise_id,))
        row_p = cursor.fetchone()
        cuenta_prov_id = row_p[0] if row_p else None
        
        if not cuenta_banco_id or not cuenta_prov_id:
            logger.warning("[Contabilidad] Falta cuenta de banco o proveedores para el pago.")
            return None

        # c) Cabecera Asiento
        cursor.execute("SELECT COALESCE(MAX(numero_asiento), 0) + 1 as proximo FROM cont_asientos WHERE enterprise_id = %s", (enterprise_id,))
        proximo_nro = cursor.fetchone()[0]

        concepto = f"Pago Importación OC#{orden_id} - Ref SWIFT: {pago_id}"
        cursor.execute("""
            INSERT INTO cont_asientos (enterprise_id, fecha, concepto, modulo_origen, comprobante_id, numero_asiento, user_id, estado)
            VALUES (%s, CURDATE(), %s, 'FONDOS', %s, %s, %s, 'CONFIRMADO')
        """, (enterprise_id, concepto, pago_id, proximo_nro, user_id))
        asiento_id = cursor.lastrowid

        # d) Detalles
        # DEBE: Proveedores (Pasivo disminuye)
        cursor.execute("""
            INSERT INTO cont_asientos_detalle (asiento_id, enterprise_id, cuenta_id, debe, haber, glosa)
            VALUES (%s, %s, %s, %s, 0, %s)
        """, (asiento_id, enterprise_id, cuenta_prov_id, monto_ars, f"Pago a Prov. Extranjero {moneda} {monto_orig} TC {tc}"))

        # HABER: Banco (Activo disminuye)
        cursor.execute("""
            INSERT INTO cont_asientos_detalle (asiento_id, enterprise_id, cuenta_id, debe, haber, glosa)
            VALUES (%s, %s, %s, 0, %s, %s)
        """, (asiento_id, enterprise_id, cuenta_banco_id, monto_ars, f"Salida por SWIFT Pago OC#{orden_id}"))

        return asiento_id

    @classmethod
    def get_supplier_performance(cls, enterprise_id):
        """Analiza el desempeño de proveedores de importación."""
        with get_db_cursor() as cursor:
            # Métricas: Lead Time, Volumen FOB, y Eficiencia (Deviaciones)
            cursor.execute("""
                SELECT 
                    t.nombre as proveedor,
                    COUNT(o.id) as total_ocs,
                    AVG(DATEDIFF(d.fecha_arribo_real, o.fecha_emision)) as avg_lead_time_days,
                    SUM(o.monto_total) as total_fob_usd,
                    SUM(CASE WHEN o.estado_importacion = 'INGRESADO' THEN 1 ELSE 0 END) as ocs_finalizadas
                FROM cmp_ordenes_compra o
                JOIN erp_terceros t ON o.proveedor_id = t.id
                LEFT JOIN imp_despachos d ON d.orden_compra_id = o.id
                WHERE o.enterprise_id = %s AND o.es_importacion = 1
                GROUP BY t.id, t.nombre
                HAVING COUNT(o.id) > 0
                ORDER BY total_fob_usd DESC
            """, (enterprise_id,))
            return cursor.fetchall()

    @classmethod
    def procesar_alertas_demora(cls):
        """
        Escanea despachos activos para enviar alertas de vencimiento de días libres.
        Ejecutado idealmente por el Cron Job de Vessel Tracking.
        """
        from datetime import date
        from services.email_service import enviar_alerta_demora, get_enterprise_email_config
        
        logger.info("[Alertas] Iniciando procesamiento de alertas de demora...")
        hoy = date.today()
        alertas_enviadas = 0

        with get_db_cursor() as cursor:
            # Seleccionar despachos con buque arribado, sin contenedor devuelto y con alerta pendiente
            cursor.execute("""
                SELECT d.id, d.enterprise_id, d.orden_compra_id, d.numero_despacho, d.fecha_arribo_real, 
                       d.dias_libres_puerto, d.costo_demora_diario_usd, 
                       o.numero_comprobante, t.nombre as buque_nombre, e.nombre as empresa_nombre
                FROM imp_despachos d
                JOIN cmp_ordenes_compra o ON d.orden_compra_id = o.id
                JOIN sys_enterprises e ON d.enterprise_id = e.id
                LEFT JOIN imp_vessel_tracking t ON d.orden_compra_id = t.orden_compra_id 
                WHERE d.fecha_arribo_real IS NOT NULL 
                  AND d.fecha_devolucion_contenedor IS NULL
                  AND d.dias_libres_puerto > 0
                  AND d.alerta_demora_enviada = 0
            """)
            despachos = cursor.fetchall()

            for d in despachos:
                # Calcular días transcurridos desde arribo
                dias_consumidos = (hoy - d['fecha_arribo_real']).days
                dias_restantes = d['dias_libres_puerto'] - dias_consumidos

                # Disparar alerta si quedan 2 días o menos (o si ya venció)
                if dias_restantes <= 2:
                    # Buscar destinatarios (Email de la empresa y administradores)
                    # Por ahora enviamos al email configurado para la empresa
                    config = get_enterprise_email_config(d['enterprise_id'])
                    destinatario = config.get('email')
                    
                    if destinatario:
                        logger.info(f"Enviando alerta demora OC {d['numero_comprobante']} a {destinatario}")
                        buque = d['buque_nombre'] or "Buque No Identificado"
                        
                        success, error = enviar_alerta_demora(
                            destinatario, "Coordinador de Logística", d['numero_despacho'],
                            buque, d['fecha_arribo_real'].strftime('%d/%m/%Y'),
                            dias_restantes, float(d['costo_demora_diario_usd'] or 0),
                            d['enterprise_id']
                        )

                        if success:
                            cursor.execute("UPDATE imp_despachos SET alerta_demora_enviada = 1 WHERE id = %s", (d['id'],))
                            alertas_enviadas += 1
                        else:
                            logger.error(f"Error enviando alerta: {error}")

        logger.info(f"[Alertas] Procesamiento finalizado. {alertas_enviadas} alertas enviadas.")
        return alertas_enviadas

