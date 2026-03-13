# ==============================================================================
# MÓDULO DE IMPORTACIONES — ETAPA 1
# Rutas: Dashboard, Tipos de Cambio (BCRA), Proveedor Internacional, Documentos
# ==============================================================================
# Este archivo es importado desde compras/routes.py:
#   from compras.importaciones_routes import register_importaciones_routes
#   register_importaciones_routes(compras_bp)
# ==============================================================================

import os
import datetime
from quart import render_template, request, g, flash, redirect, url_for, jsonify
from core.decorators import login_required
from database import get_db_cursor, atomic_transaction
from werkzeug.utils import secure_filename


def register_importaciones_routes(bp):
    """Registra todas las rutas del módulo de Importaciones en el Blueprint dado."""

    # ─────────────────────────────────────────────────────────────────────────
    # DASHBOARD
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/importaciones')
    @login_required
    async def importaciones_dashboard():
        """Vista principal del módulo de Importaciones."""
        from services.bcra_service import CurrencyRateService
        ent_id = g.user['enterprise_id']

        async with get_db_cursor(dictionary=True) as cursor:
            # Órdenes marcadas como importación
            try:
                await cursor.execute("""
                    SELECT o.*, p.nombre as proveedor_nombre,
                           p.pais_origen, p.codigo_pais_iso
                    FROM cmp_ordenes_compra o
                    JOIN erp_terceros p ON o.proveedor_id = p.id
                    WHERE o.enterprise_id = %s AND o.es_importacion = 1
                    ORDER BY o.fecha_emision DESC LIMIT 20
                """, (ent_id,))
                ordenes_imp = await cursor.fetchall()
            except Exception:
                ordenes_imp = []

            # Documentos pendientes
            try:
                await cursor.execute("""
                    SELECT d.*, p.nombre as proveedor_nombre
                    FROM imp_documentos d
                    LEFT JOIN erp_terceros p ON d.proveedor_id = p.id
                    WHERE d.enterprise_id = %s AND d.estado IN ('PENDIENTE', 'OBSERVADO')
                    ORDER BY d.created_at DESC
                """, (ent_id,))
                docs_pendientes = await cursor.fetchall()
            except Exception:
                docs_pendientes = []

        try:
            tipos_cambio = await CurrencyRateService.get_all_vigentes(ent_id)
            for t in tipos_cambio:
                for k in ['fecha', 'created_at']:
                    if hasattr(t.get(k), 'isoformat'):
                        t[k] = t[k].isoformat()
        except Exception:
            tipos_cambio = []

        return await render_template('compras/importaciones_dashboard.html',
                               ordenes_imp=ordenes_imp,
                               docs_pendientes=docs_pendientes,
                               tipos_cambio=tipos_cambio)

    # ─────────────────────────────────────────────────────────────────────────
    # API — TIPOS DE CAMBIO BCRA
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/api/tipos-cambio')
    @login_required
    async def api_tipos_cambio():
        """
        GET /compras/api/tipos-cambio
        Parámetros opcionales: moneda=USD, tipo=OFICIAL_VENDEDOR, actualizar=1
        """
        from services.bcra_service import CurrencyRateService
        moneda     = request.args.get('moneda')
        tipo       = request.args.get('tipo')
        actualizar = request.args.get('actualizar', '0') == '1'
        try:
            if actualizar:
                stats = await CurrencyRateService.actualizar_cotizaciones_hoy(g.user['enterprise_id'])
            todos = await CurrencyRateService.get_all_vigentes(g.user['enterprise_id'])
            if moneda:
                todos = [t for t in todos if t['moneda'] == moneda.upper()]
            if tipo:
                todos = [t for t in todos if t['tipo'] == tipo.upper()]
            for t in todos:
                for k in ['fecha', 'created_at']:
                    if hasattr(t.get(k), 'isoformat'):
                        t[k] = t[k].isoformat()
            return jsonify({'success': True, 'data': todos, 'total': len(todos)})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    @bp.route('/compras/api/tipos-cambio/manual', methods=['POST'])
    @login_required
    async def api_tipos_cambio_manual():
        """Registra un tipo de cambio manual."""
        from services.bcra_service import CurrencyRateService
        data   = (await request.json) or {}
        moneda = data.get('moneda', 'USD').upper()
        tipo   = data.get('tipo', 'OFICIAL_VENDEDOR').upper()
        valor  = float(data.get('valor', 0) or 0)
        fecha  = data.get('fecha')
        if valor <= 0:
            return jsonify({'success': False, 'message': 'El valor debe ser mayor a 0'}), 400
        try:
            await CurrencyRateService.registrar_manual(
                g.user['enterprise_id'], moneda, tipo, valor, fecha, g.user['id']
            )
            return jsonify({'success': True, 'message': f'{moneda}/{tipo} = {valor} registrado.'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    # ─────────────────────────────────────────────────────────────────────────
    # PROVEEDOR INTERNACIONAL
    # ─────────────────────────────────────────────────────────────────────────

    PAISES_ISO = [
        ("CN", "China"), ("US", "Estados Unidos"), ("BR", "Brasil"),
        ("DE", "Alemania"), ("IT", "Italia"), ("ES", "Espana"),
        ("FR", "Francia"), ("GB", "Reino Unido"), ("JP", "Japon"),
        ("KR", "Corea del Sur"), ("IN", "India"), ("MX", "Mexico"),
        ("CL", "Chile"), ("UY", "Uruguay"), ("PY", "Paraguay"),
        ("BO", "Bolivia"), ("PE", "Peru"), ("CO", "Colombia"),
        ("TR", "Turquia"), ("TW", "Taiwan"), ("HK", "Hong Kong"),
        ("OTHER", "Otro"),
    ]
    MONEDAS_IMP = [
        ("USD", "Dolar (USD)"), ("EUR", "Euro (EUR)"), ("BRL", "Real (BRL)"),
        ("CNY", "Yuan (CNY)"), ("GBP", "Libra (GBP)"), ("JPY", "Yen (JPY)"),
        ("ARS", "Peso Arg. (ARS)"),
    ]

    @bp.route('/compras/proveedores/nuevo-internacional', methods=['GET', 'POST'])
    @login_required
    @atomic_transaction('compras')
    async def nuevo_proveedor_internacional():
        """Alta de proveedor extranjero sin CUIT argentino."""
        if request.method == 'POST':
            nombre               = (await request.form).get('nombre', '').strip()
            pais_origen          = (await request.form).get('pais_origen', '').upper()
            codigo_pais_iso      = (await request.form).get('codigo_pais_iso', '').upper()
            identificador_fiscal = (await request.form).get('identificador_fiscal', '')
            codigo_swift         = (await request.form).get('codigo_swift', '')
            moneda_operacion     = (await request.form).get('moneda_operacion', 'USD').upper()
            email                = (await request.form).get('email', '')
            telefono             = (await request.form).get('telefono', '')
            web                  = (await request.form).get('web', '')
            observaciones        = (await request.form).get('observaciones', '')
            codigo               = (await request.form).get('codigo', '')

            if not nombre:
                await flash("El nombre del proveedor es obligatorio.", "danger")
                return redirect(url_for('compras.nuevo_proveedor_internacional'))

            # CUIT placeholder: EXT-CN-TAXID12345
            sufijo = (identificador_fiscal or nombre)[:10].replace(' ', '')
            cuit_placeholder = f"EXT-{codigo_pais_iso}-{sufijo}"

            try:
                async with get_db_cursor(dictionary=True) as cursor:
                    await cursor.execute("""
                        INSERT INTO erp_terceros (
                            enterprise_id, codigo, nombre, cuit, email, telefono,
                            observaciones, es_cliente, es_proveedor, tipo_responsable,
                            es_proveedor_extranjero, pais_origen, codigo_pais_iso,
                            identificador_fiscal, codigo_swift, moneda_operacion, web
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 0, 1, 'PROVEEDOR_EXTERIOR',
                                  1, %s, %s, %s, %s, %s, %s)
                    """, (
                        g.user['enterprise_id'], codigo or None, nombre, cuit_placeholder,
                        email, telefono, observaciones,
                        pais_origen, codigo_pais_iso, identificador_fiscal,
                        codigo_swift, moneda_operacion, web
                    ))
                    new_id = cursor.lastrowid
                await flash(f"Proveedor internacional '{nombre}' registrado exitosamente.", "success")
                return redirect(url_for('compras.perfil_proveedor', id=new_id))
            except Exception as e:
                await flash(f"Error al registrar: {str(e)}", "danger")
                return redirect(url_for('compras.nuevo_proveedor_internacional'))

        return await render_template('compras/proveedor_internacional_form.html',
                               paises=PAISES_ISO, monedas=MONEDAS_IMP)

    # ─────────────────────────────────────────────────────────────────────────
    # DOCUMENTOS DE IMPORTACIÓN
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/importaciones/documentos/agregar', methods=['POST'])
    @login_required
    async def importacion_agregar_documento():
        """Registra un documento del circuito de importación."""
        ent_id           = g.user['enterprise_id']
        orden_id         = (await request.form).get('orden_compra_id') or None
        tipo_documento   = (await request.form).get('tipo_documento', 'OTRO')
        numero_documento = (await request.form).get('numero_documento', '')
        fecha_documento  = (await request.form).get('fecha_documento') or None
        monto            = (await request.form).get('monto') or None
        moneda           = (await request.form).get('moneda', 'USD').upper()
        proveedor_id     = (await request.form).get('proveedor_id') or None
        descripcion      = (await request.form).get('descripcion', '')

        archivo_path = None
        if 'archivo' in (await request.files):
            f = (await request.files)['archivo']
            if f and f.filename:
                ext = os.path.splitext(f.filename)[1].lower()
                if ext in ['.pdf', '.jpg', '.jpeg', '.png', '.xlsx', '.xls']:
                    fname  = secure_filename(f"IMP_{ent_id}_{tipo_documento}_{f.filename}")
                    folder = os.path.join(os.getcwd(), 'static', 'uploads', 'importaciones')
                    os.makedirs(folder, exist_ok=True)
                    await f.save(os.path.join(folder, fname))
                    archivo_path = f"uploads/importaciones/{fname}"

        try:
            async with get_db_cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO imp_documentos (
                        enterprise_id, orden_compra_id, tipo_documento, numero_documento,
                        fecha_documento, monto, moneda, proveedor_id,
                        descripcion, archivo_path, estado, user_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDIENTE', %s)
                """, (
                    ent_id, orden_id, tipo_documento, numero_documento,
                    fecha_documento, monto, moneda, proveedor_id,
                    descripcion, archivo_path, g.user['id']
                ))
            await flash("Documento registrado correctamente.", "success")
        except Exception as e:
            await flash(f"Error al guardar documento: {str(e)}", "danger")

        return redirect(request.referrer or url_for('compras.importaciones_dashboard'))

    @bp.route('/compras/api/importaciones/documentos/<int:orden_id>')
    @login_required
    async def api_documentos_importacion(orden_id):
        """API: documentos asociados a una orden de importación."""
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("""
                SELECT d.*, p.nombre as proveedor_nombre
                FROM imp_documentos d
                LEFT JOIN erp_terceros p ON d.proveedor_id = p.id
                WHERE d.orden_compra_id = %s AND d.enterprise_id = %s
                ORDER BY d.tipo_documento, d.fecha_documento DESC
            """, (orden_id, g.user['enterprise_id']))
            docs = await cursor.fetchall()
        for d in docs:
            for k in ['fecha_documento', 'fecha_vencimiento', 'created_at', 'updated_at']:
                if hasattr(d.get(k), 'isoformat'):
                    d[k] = d[k].isoformat()
        return jsonify({'success': True, 'data': docs})

    # ─────────────────────────────────────────────────────────────────────────
    # CONVERTIR ORDEN → IMPORTACIÓN
    # ─────────────────────────────────────────────────────────────────────────

    @bp.route('/compras/api/importaciones/orden/<int:orden_id>/marcar', methods=['POST'])
    @login_required
    @atomic_transaction('IMPORTACIONES')
    async def api_marcar_orden_importacion(orden_id):
        """Convierte una Orden de Compra estándar en una Orden de Importación."""
        data = (await request.json) or {}
        try:
            async with get_db_cursor() as cursor:
                await cursor.execute("""
                    UPDATE cmp_ordenes_compra
                    SET es_importacion = 1,
                        moneda          = %s,
                        incoterm        = %s,
                        pais_origen     = %s,
                        puerto_embarque = %s,
                        puerto_destino  = %s,
                        tipo_cambio_valor = %s
                WHERE id = %s AND enterprise_id = %s
            """, (
                data.get('moneda', 'USD').upper(),
                data.get('incoterm', 'FOB').upper(),
                data.get('pais_origen', ''),
                data.get('puerto_embarque', ''),
                data.get('puerto_destino', ''),
                data.get('tipo_cambio_valor') if data.get('tipo_cambio_valor') not in [None, ''] else None,
                orden_id, g.user['enterprise_id']
            ))
            return jsonify({'success': True, 'message': 'Orden marcada como importación.'})
        except Exception as e:
            # error log is handled by decorator, but we return pretty JSON
            return jsonify({'success': False, 'message': str(e)}), 500
