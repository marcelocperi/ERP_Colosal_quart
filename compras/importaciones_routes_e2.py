# ==============================================================================
# MÓDULO DE IMPORTACIONES — ETAPA 2
# Cargos, CUI, Despacho Aduanero, Ingreso al Stock
# Se registra desde importaciones_routes.py → register_importaciones_routes_e2()
# ==============================================================================

from quart import render_template, request, g, flash, redirect, url_for, jsonify
from core.decorators import login_required
from database import get_db_cursor, atomic_transaction
from services.importacion_service import ImportacionService


def register_importaciones_routes_e2(bp):
    """Etapa 2: Cargos + CUI + Despacho + Ingreso al Stock."""

    # ─────────────────────────────────────────────────────────────────────────
    # DETALLE DE ORDEN DE IMPORTACIÓN  (vista central de Etapa 2)
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/importaciones/orden/<int:orden_id>')
    @login_required
    async def importacion_orden_detalle(orden_id):
        """Vista detallada de una Orden de Importación: cargos, CUI, despacho."""
        ent_id = g.user['enterprise_id']

        async with get_db_cursor(dictionary=True) as cursor:
            # Cabecera de la OC
            await cursor.execute("""
                SELECT o.*, p.nombre as proveedor_nombre, p.pais_origen,
                       p.codigo_pais_iso, p.moneda_operacion, p.codigo_swift
                FROM cmp_ordenes_compra o
                JOIN erp_terceros p ON o.proveedor_id = p.id
                WHERE o.id = %s AND o.enterprise_id = %s
            """, (orden_id, ent_id))
            orden = await cursor.fetchone()

            if not orden:
                await flash("Orden no encontrada.", "danger")
                return redirect(url_for('compras.importaciones_dashboard'))

            # Documentos de la OC
            await cursor.execute("""
                SELECT * FROM imp_documentos
                WHERE orden_compra_id = %s AND enterprise_id = %s
                ORDER BY tipo_documento, fecha_documento
            """, (orden_id, ent_id))
            documentos = await cursor.fetchall()

            # Proveedores activos para selector despachante
            await cursor.execute("""
                SELECT id, nombre FROM erp_terceros
                WHERE enterprise_id = %s AND es_proveedor = 1
                ORDER BY nombre
            """, (ent_id,))
            proveedores = await cursor.fetchall()

            # Depósitos disponibles
            await cursor.execute("""
                SELECT id, nombre FROM stk_depositos
                WHERE enterprise_id = %s AND activo = 1
                ORDER BY predeterminado DESC, nombre
            """, (ent_id,))
            depositos = await cursor.fetchall()

            # Bancos para pagos
            await cursor.execute("""
                SELECT id, nombre FROM fin_bancos 
                WHERE (enterprise_id = %s OR enterprise_id = 0)
                ORDER BY nombre
            """, (ent_id,))
            bancos = await cursor.fetchall()

        # Datos del servicio (pueden fallar si la tabla no tiene items aún)
        cargos       = await ImportacionService.get_cargos_orden(orden_id, ent_id)
        despacho     = await ImportacionService.get_despacho(orden_id, ent_id)
        items_orden  = await ImportacionService.get_items_orden(orden_id, ent_id)
        
        # Etapa 4: Logística Stats
        logistica_stats = ImportacionService.get_logistica_stats(despacho)

        # Serializar fechas
        for obj_list in [cargos, documentos, items_orden, [despacho] if despacho else []]:
            for obj in obj_list:
                if obj:
                    for k in ['fecha', 'fecha_documento', 'fecha_oficializacion',
                              'fecha_liberacion', 'fecha_embarque', 'fecha_arribo_estimada', 
                              'fecha_arribo_real', 'created_at', 'updated_at']:
                        if hasattr(obj.get(k), 'isoformat'):
                            obj[k] = obj[k].isoformat()

        import datetime
        hoy = datetime.date.today().strftime('%Y-%m-%d')

        # Etapa 5.2: Desvios de costos
        desvios = await ImportacionService.get_desvio_costos(orden_id, ent_id)

        # Fase 1: Tracking de Buque
        from services.vessel_tracking_service import VesselTrackingService
        vessel_track = await VesselTrackingService.get_last_tracking(orden_id, ent_id)

        return await render_template('compras/importacion_orden_detalle.html',
                               orden=orden,
                               cargos=cargos,
                               despacho=despacho,
                               vessel_track=vessel_track,
                               logistica_stats=logistica_stats, # <-- Etapa 4
                               desvios=desvios, # <-- Etapa 5.2
                               documentos=documentos,
                               items_orden=items_orden,
                               proveedores=proveedores,
                               depositos=depositos,
                               bancos=bancos,
                               hoy=hoy,
                               tipos_cargo=ImportacionService.TIPOS_CARGO)



    # ─────────────────────────────────────────────────────────────────────────
    # CARGOS DE IMPORTACIÓN
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/importaciones/orden/<int:orden_id>/cargo/agregar', methods=['POST'])
    @login_required
    async def importacion_agregar_cargo(orden_id):
        """Registra un cargo de importación (flete, derechos, despachante, etc.)."""
        from services.bcra_service import CurrencyRateService
        ent_id = g.user['enterprise_id']

        tipo_cargo   = (await request.form).get('tipo_cargo', 'OTRO')
        descripcion  = (await request.form).get('descripcion', tipo_cargo)
        monto_orig   = float((await request.form).get('monto_orig', 0) or 0)
        moneda_orig  = (await request.form).get('moneda_orig', 'ARS').upper()
        proveedor_id = (await request.form).get('proveedor_id') or None
        fecha        = (await request.form).get('fecha') or None
        aplica_cui   = 1 if (await request.form).get('aplica_a_cui') == '1' else 0
        es_estimado  = 1 if (await request.form).get('es_estimado') == '1' else 0
        ref_id       = (await request.form).get('cargo_referencia_id') or None

        # Tipo de cambio
        from services.bcra_service import CurrencyRateService
        tc_manual = float((await request.form).get('tipo_cambio', 0) or 0)
        if tc_manual <= 0 and moneda_orig != 'ARS':
            tc_manual = await CurrencyRateService.get_tipo_cambio(moneda_orig) or 1000.0
        elif moneda_orig == 'ARS':
            tc_manual = 1.0

        if monto_orig <= 0:
            await flash("El monto del cargo debe ser mayor a cero.", "danger")
            return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

        try:
            await ImportacionService.agregar_cargo(
                ent_id, orden_id, tipo_cargo, descripcion,
                monto_orig, moneda_orig, tc_manual,
                proveedor_id, fecha, g.user['id'], aplica_cui,
                es_estimado, ref_id
            )


            await flash(f"Cargo '{descripcion}' registrado correctamente.", "success")
        except Exception as e:
            await flash(f"Error al registrar cargo: {str(e)}", "danger")

        return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

    @bp.route('/compras/importaciones/cargo/<int:cargo_id>/eliminar', methods=['POST'])
    @login_required
    async def importacion_eliminar_cargo(cargo_id):
        """Elimina un cargo si el despacho no está ingresado."""
        ent_id = g.user['enterprise_id']
        try:
            async with get_db_cursor(dictionary=True) as cursor:
                await cursor.execute("""
                    SELECT orden_compra_id FROM imp_cargos
                    WHERE id = %s AND enterprise_id = %s
                """, (cargo_id, ent_id))
                row = await cursor.fetchone()
                if not row:
                    return jsonify({'success': False, 'message': 'Cargo no encontrado'}), 404
                orden_id = row['orden_compra_id']

                await cursor.execute("""
                    DELETE FROM imp_cargos WHERE id = %s AND enterprise_id = %s
                """, (cargo_id, ent_id))
            return jsonify({'success': True, 'orden_id': orden_id})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    # ─────────────────────────────────────────────────────────────────────────
    # API: CÁLCULO CUI
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/api/importaciones/orden/<int:orden_id>/cui')
    @login_required
    async def api_calcular_cui(orden_id):
        """
        Calcula y retorna el CUI (Costo Unitario de Importación) por artículo.
        Parámetro opcional: tipo_cambio=1050.00
        """
        tc = float(request.args.get('tipo_cambio', 0) or 0) or None
        try:
            resultado = await ImportacionService.calcular_cui(
                orden_id, g.user['enterprise_id'], tc
            )
            return jsonify({'success': True, 'data': resultado})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    # ─────────────────────────────────────────────────────────────────────────
    # DESPACHO ADUANERO
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/importaciones/orden/<int:orden_id>/despacho/guardar', methods=['POST'])
    @login_required
    async def importacion_guardar_despacho(orden_id):
        """Crea o actualiza el despacho aduanero de una OC de importación."""
        ent_id = g.user['enterprise_id']
        data = {
            'numero_despacho':            (await request.form).get('numero_despacho', ''),
            'despachante_id':             (await request.form).get('despachante_id') or None,
            'fecha_oficializacion':       (await request.form).get('fecha_oficializacion') or None,
            'canal':                      (await request.form).get('canal', 'VERDE'),
            'estado':                     (await request.form).get('estado', 'PENDIENTE'),
            'valor_fob_usd':              (await request.form).get('valor_fob_usd', 0),
            'valor_cif_usd':              (await request.form).get('valor_cif_usd', 0),
            'derechos_ars':               (await request.form).get('derechos_ars', 0),
            'tasa_estadistica_ars':       (await request.form).get('tasa_estadistica_ars', 0),
            'otros_tributos_ars':         (await request.form).get('otros_tributos_ars', 0),
            'tipo_cambio_oficializacion': (await request.form).get('tipo_cambio_oficializacion', 0),
            'observaciones':              (await request.form).get('observaciones', ''),
            # Etapa 4: Logística
            'transportista':              (await request.form).get('transportista'),
            'guia_bl_tracking':           (await request.form).get('guia_bl_tracking'),
            'fecha_embarque':             (await request.form).get('fecha_embarque') or None,
            'fecha_arribo_estimada':      (await request.form).get('fecha_arribo_estimada') or None,
            'fecha_arribo_real':          (await request.form).get('fecha_arribo_real') or None,
            'puerto_embarque':            (await request.form).get('puerto_embarque'),
            'puerto_destino':             (await request.form).get('puerto_destino'),
            'bultos':                     (await request.form).get('bultos', 0),
            'peso_kg':                    (await request.form).get('peso_kg', 0),
        }

        try:
            despacho_id = await ImportacionService.crear_o_actualizar_despacho(
                ent_id, orden_id, data, g.user['id']
            )
            await flash(f"Despacho #{data['numero_despacho'] or despacho_id} guardado correctamente.", "success")
        except Exception as e:
            await flash(f"Error al guardar despacho: {str(e)}", "danger")

        return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

    # ─────────────────────────────────────────────────────────────────────────
    # INGRESO AL STOCK (acción final del circuito)
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/importaciones/orden/<int:orden_id>/ingresar-stock', methods=['POST'])
    @login_required
    @atomic_transaction('compras')
    async def importacion_ingresar_stock(orden_id):
        """
        Registra el ingreso definitivo al stock con el CUI calculado.
        Requiere que exista un despacho en estado LIBERADO.
        """
        ent_id     = g.user['enterprise_id']
        deposito_id = (await request.form).get('deposito_id')
        tc_manual   = float((await request.form).get('tipo_cambio', 0) or 0) or None

        if not deposito_id:
            await flash("Debe seleccionar el depósito de destino.", "danger")
            return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

        # Obtener despacho activo
        despacho = await ImportacionService.get_despacho(orden_id, ent_id)
        if not despacho:
            await flash("No existe un Despacho Aduanero registrado para esta orden.", "danger")
            return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

        if despacho.get('estado') not in ('LIBERADO', 'INGRESADO'):
            await flash(f"El despacho debe estar en estado LIBERADO para ingresar al stock. "
                  f"Estado actual: {despacho.get('estado')}", "warning")
            return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

        if despacho.get('estado') == 'INGRESADO':
            await flash("Esta importación ya fue ingresada al stock anteriormente.", "warning")
            return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

        try:
            resultado = await ImportacionService.registrar_ingreso_stock(
                orden_compra_id=orden_id,
                despacho_id=despacho['id'],
                enterprise_id=ent_id,
                deposito_id=int(deposito_id),
                user_id=g.user['id'],
                tipo_cambio_usd=tc_manual
            )

            if resultado['success']:
                cant = len(resultado['articulos_ingresados'])
                total = resultado['total_cui_ars']
                await flash(
                    f"✅ Importación ingresada al stock: {cant} artículo(s). "
                    f"Costo total CUI = $ {total:,.2f} ARS (TC: {resultado['tipo_cambio']:.2f})",
                    "success"
                )
            else:
                await flash(f"Error en el ingreso: {resultado.get('message')}", "danger")

        except Exception as e:
            await flash(f"Error inesperado al ingresar importación: {str(e)}", "danger")

        return redirect(url_for('compras.importacion_orden_detalle', orden_id=orden_id))

    # ─────────────────────────────────────────────────────────────────────────
    # API: MARCAR DESPACHO LIBERADO
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/api/importaciones/despacho/<int:despacho_id>/liberar', methods=['POST'])
    @login_required
    async def api_liberar_despacho(despacho_id):
        """Marca el despacho como LIBERADO (canal verde/azul superado)."""
        ent_id = g.user['enterprise_id']
        try:
            async with get_db_cursor() as cursor:
                await cursor.execute("""
                    UPDATE imp_despachos SET estado = 'LIBERADO', fecha_liberacion = CURDATE()
                    WHERE id = %s AND enterprise_id = %s
                """, (despacho_id, ent_id))
                if cursor.rowcount == 0:
                    return jsonify({'success': False, 'message': 'Despacho no encontrado'}), 404

                # Sync orden
                await cursor.execute("""
                    UPDATE cmp_ordenes_compra
                    SET estado_importacion = 'LIBERADO'
                    WHERE id = (SELECT orden_compra_id FROM imp_despachos WHERE id = %s)
                      AND enterprise_id = %s
                """, (despacho_id, ent_id))

            return jsonify({'success': True, 'message': 'Despacho liberado correctamente.'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    # ─────────────────────────────────────────────────────────────────────────
    # API: RESUMEN DE COSTOS DE IMPORTACIÓN (para widgets del dashboard)
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/api/importaciones/orden/<int:orden_id>/resumen')
    @login_required
    async def api_resumen_importacion(orden_id):
        """Retorna resumen de costos + CUI para la OC de importación."""
        ent_id = g.user['enterprise_id']
        try:
            cargos   = await ImportacionService.get_cargos_orden(orden_id, ent_id)
            despacho = await ImportacionService.get_despacho(orden_id, ent_id)
            cui      = await ImportacionService.calcular_cui(orden_id, ent_id)

            # Serializar
            for obj_list in [cargos, [despacho] if despacho else []]:
                for obj in obj_list:
                    if obj:
                        for k in ['fecha', 'fecha_oficializacion', 'fecha_liberacion',
                                  'created_at', 'updated_at']:
                            if hasattr(obj.get(k), 'isoformat'):
                                obj[k] = obj[k].isoformat()

            return jsonify({
                'success': True,
                'cargos':           cargos,
                'despacho':         despacho,
                'cui_resultado':    cui,
            })
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    # ─────────────────────────────────────────────────────────────────────────
    # STAGE 5: AVANCED DASHBOARD
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/importaciones/dashboard-avanzado')
    @login_required
    async def importaciones_dashboard_avanzado():
        """Dashboard Ejecutivo de Importaciones (Stage 5)."""
        ent_id = g.user['enterprise_id']
        stats = await ImportacionService.get_dashboard_stats(ent_id)
        
        return await render_template('compras/importaciones_dashboard_avanzado.html', 
                               stats=stats)

    @bp.route('/compras/api/importaciones/orden/<int:orden_id>/desvios')
    @login_required
    async def api_desviacion_costos(orden_id):
        """API para análisis de desvíos entre estimado y real."""
        ent_id = g.user['enterprise_id']
        try:
            stats = await ImportacionService.get_desvio_costos(orden_id, ent_id)
            return jsonify({'success': True, 'data': stats})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500


    @bp.route('/compras/api/importaciones/orden/<int:orden_id>/track-vessel', methods=['POST'])
    @login_required
    async def api_track_vessel(orden_id):
        """API para rastrear buque AIS."""
        from services.vessel_tracking_service import VesselTrackingService
        ent_id = g.user['enterprise_id']
        mmsi = (await request.json).get('mmsi')
        
        try:
            # Persistir MMSI en imp_despachos para futuras actualizaciones automáticas
            from database import get_db_cursor
            async with get_db_cursor() as cursor:
                await cursor.execute("""
                    UPDATE imp_despachos SET vessel_mmsi = %s 
                    WHERE orden_compra_id = %s AND enterprise_id = %s
                """, (mmsi, orden_id, ent_id))

            res = await VesselTrackingService.track_vessel_by_mmsi(ent_id, orden_id, mmsi, g.user['id'])
            return jsonify(res)
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
