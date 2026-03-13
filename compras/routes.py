from quart import Blueprint, render_template, request, g, flash, redirect, url_for, jsonify
from core.decorators import login_required, permission_required
from database import get_db_cursor, atomic_transaction
from services.tercero_service import TerceroService
from services.georef_service import GeorefService
from services.email_service import enviar_notificacion_retencion, _enviar_email
from services.purchase_order_mailer import PurchaseOrderMailer
from services.budget_service import BudgetService
from services.receiving_service import ReceivingService
from services.afip_service import AfipService
from xhtml2pdf import pisa
import io, re, datetime, asyncio

compras_bp = Blueprint('compras', __name__, template_folder='templates')

# ── Módulo de Importaciones ──────────────────────────────────────────────────
from compras.importaciones_routes import register_importaciones_routes
register_importaciones_routes(compras_bp)
from compras.importaciones_routes_e2 import register_importaciones_routes_e2
register_importaciones_routes_e2(compras_bp)
from compras.importaciones_routes_e3 import register_importaciones_routes_e3
register_importaciones_routes_e3(compras_bp)

# ── Módulo Industrial (Costos e Ingeniería) movido a produccion ──────────────────

from compras.rfq_routes import register_rfq_routes
register_rfq_routes(compras_bp)

from compras.fazon_routes import register_fazon_routes
register_fazon_routes(compras_bp)
# ────────────────────────────────────────────────────────────────────────────

@compras_bp.route('/compras/reposicion', methods=['GET'])
@login_required
@permission_required('compras.gestionar_reposicion')
async def reposicion_dashboard():
    """Tablero de Detección de Faltantes agrupado por proveedor."""
    ent_id = g.user['enterprise_id']
    origen_id_filter = request.args.get('origen_id', '')
    proveedor_id_filter = request.args.get('proveedor_id', '')
    
    async with get_db_cursor(dictionary=True) as cursor:
        # 1. Cargar comboBox para filtros
        await cursor.execute("SELECT id, nombre FROM cmp_sourcing_origenes WHERE activo = 1 ORDER BY nombre")
        origenes = await cursor.fetchall()
        
        await cursor.execute("SELECT id, nombre FROM erp_terceros WHERE (enterprise_id = %s OR enterprise_id = 0) AND es_proveedor = 1 ORDER BY nombre", (ent_id,))
        proveedores_dd = await cursor.fetchall()

        # 2. Construir Query de faltantes con filtros
        filtros_sql = ""
        params = [ent_id]
        if origen_id_filter:
            filtros_sql += " AND ap.origen_id = %s"
            params.append(origen_id_filter)
        if proveedor_id_filter:
            filtros_sql += " AND ap.proveedor_id = %s"
            params.append(proveedor_id_filter)

        sql = f"""
            SELECT 
                stk_articulos.id, stk_articulos.codigo, stk_articulos.nombre, stk_articulos.punto_pedido, stk_articulos.stock_minimo, stk_articulos.cant_min_pedido,
                COALESCE(SUM(stk_existencias.cantidad), 0) as stock_actual,
                COALESCE(
                    CEIL(GREATEST((stk_articulos.punto_pedido - COALESCE(SUM(stk_existencias.cantidad), 0)), 0.0001) / NULLIF(stk_articulos.cant_min_pedido, 0)) * stk_articulos.cant_min_pedido,
                    GREATEST((stk_articulos.punto_pedido - COALESCE(SUM(stk_existencias.cantidad), 0)), 0)
                ) as sugerido,
                cmp_articulos_proveedores.proveedor_id, erp_terceros.nombre as proveedor_nombre, erp_terceros.email as proveedor_email, cmp_articulos_proveedores.lead_time_dias,
                cmp_articulos_proveedores.es_habitual, erp_terceros.es_proveedor_extranjero
            FROM stk_articulos
            LEFT JOIN stk_existencias ON stk_articulos.id = stk_existencias.articulo_id AND stk_articulos.enterprise_id = stk_existencias.enterprise_id
            LEFT JOIN cmp_articulos_proveedores ON stk_articulos.id = cmp_articulos_proveedores.articulo_id AND stk_articulos.enterprise_id = cmp_articulos_proveedores.enterprise_id AND cmp_articulos_proveedores.es_habitual = 1
            LEFT JOIN erp_terceros ON cmp_articulos_proveedores.proveedor_id = erp_terceros.id
            WHERE stk_articulos.enterprise_id = %s AND stk_articulos.activo = 1 {filtros_sql}
            GROUP BY stk_articulos.id, stk_articulos.codigo, stk_articulos.nombre, stk_articulos.punto_pedido, stk_articulos.stock_minimo, stk_articulos.cant_min_pedido, cmp_articulos_proveedores.proveedor_id, erp_terceros.nombre, erp_terceros.email, cmp_articulos_proveedores.lead_time_dias, cmp_articulos_proveedores.es_habitual, erp_terceros.es_proveedor_extranjero
            HAVING COALESCE(SUM(stk_existencias.cantidad), 0) <= stk_articulos.punto_pedido 
               AND stk_articulos.punto_pedido > 0
            ORDER BY erp_terceros.nombre, (stk_articulos.punto_pedido - COALESCE(SUM(stk_existencias.cantidad), 0)) DESC
        """
        await cursor.execute(sql, tuple(params))
        faltantes = await cursor.fetchall()
        
    # 3. Agrupar por proveedor en Python
    grupos = {}
    for f in faltantes:
        pid = f['proveedor_id'] or 0
        pnom = f['proveedor_nombre'] or 'Sin Asignar'
        pemail = f.get('proveedor_email')
        if pid not in grupos:
            grupos[pid] = {
                'id': pid, 
                'nombre': pnom, 
                'email': pemail,
                'es_extranjero': f['es_proveedor_extranjero'] == 1, 
                'faltantes': []
            }
        grupos[pid]['faltantes'].append(f)

    # Convertir dictionary en array para que jinja lo itere mas facil
    grupos_list = sorted(grupos.values(), key=lambda x: (x['id'] == 0, x['nombre']))
        
    return await render_template('compras/reposicion_dashboard.html', 
                            grupos=grupos_list, 
                            origenes=origenes, 
                            proveedores_dd=proveedores_dd,
                            origen_id_filter=origen_id_filter,
                            proveedor_id_filter=proveedor_id_filter)

@compras_bp.route('/compras/api/reposicion/generar_cotizacion_lote', methods=['POST'])
@login_required
@permission_required('compras.gestionar_reposicion')
@atomic_transaction('COMPRAS')
async def api_reposicion_generar_cotizacion_lote():
    """Genera una Solicitud de Cotización (RFQ) con varios artículos de un mismo proveedor."""
    data = (await request.json) or {}
    proveedor_id = data.get('proveedor_id')
    items = data.get('items', [])
    fecha_vencimiento = data.get('fecha_vencimiento') 
    
    if not proveedor_id or not items:
        return jsonify({'success': False, 'message': 'Faltan datos (proveedor o artículos).'}), 400
        
    if not fecha_vencimiento:
        fecha_vencimiento = (datetime.datetime.now() + datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        
    ent_id = g.user['enterprise_id']
    uid = g.user['id']
    
    async with get_db_cursor(dictionary=True) as cursor:
        from services.quotation_mailer import QuotationMailer
        mailer = QuotationMailer(ent_id)
        
        # Check if provider has an email
        await cursor.execute("SELECT codigo, nombre, email FROM erp_terceros WHERE id = %s", (proveedor_id,))
        prov = await cursor.fetchone()
        
        if not prov or not prov['email']:
            nombre_prov = prov['nombre'] if prov else f"Proveedor {proveedor_id}"
            return jsonify({
                'success': False, 
                'error_type': 'missing_email',
                'message': f'Complete direccion de correo del proveedor para poder generar RFQ ',
                'nombre_proveedor': nombre_prov
            }), 400

        sec_hash = await mailer.generate_security_hash(prov['codigo'] if prov else f"PROV{proveedor_id}")
        
        await cursor.execute("""
            INSERT INTO cmp_cotizaciones (enterprise_id, proveedor_id, fecha_envio, fecha_vencimiento, estado, security_hash, user_id)
            VALUES (%s, %s, NOW(), %s, 'ENVIADA', %s, %s)
        """, (ent_id, proveedor_id, fecha_vencimiento, sec_hash, uid))
        cot_id = cursor.lastrowid
        
        for it in items:
            await cursor.execute("""
                INSERT INTO cmp_items_cotizacion (enterprise_id, cotizacion_id, articulo_id, cantidad, user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (ent_id, cot_id, it['articulo_id'], it['cantidad'], uid))
            
    return jsonify({
        'success': True, 
        'message': f'Solicitud de Cotización #{cot_id} generada exitosamente.',
        'cot_id': cot_id
    })
@compras_bp.route('/compras/api/reposicion/rechazar', methods=['POST'])
@login_required
@permission_required('compras.gestionar_reposicion')
@atomic_transaction('COMPRAS')
async def api_reposicion_rechazar():
    """Loguea el rechazo de una sugerencia de compra con su motivo."""
    data = (await request.json) or {}
    articulo_id = data.get('articulo_id')
    motivo = data.get('motivo')
    
    if not articulo_id or not motivo:
        return jsonify({'success': False, 'message': 'Faltan datos.'}), 400
        
        from quart import session, g
        sid = getattr(g, 'sid', None) or session.get('session_id')
        async with get_db_cursor() as cursor:
            await cursor.execute("""
                INSERT INTO sys_transaction_logs (enterprise_id, user_id, session_id, module, endpoint, request_method, status, severity, impact_category, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (g.user['enterprise_id'], g.user['id'], sid, 'COMPRAS', '/compras/api/reposicion/rechazar', 'POST', 
                  'SUCCESS', 2, 'Operational', f"SUGERENCIA_RECHAZADA: Articulo ID {articulo_id}. Motivo: {motivo}"))
              
    return jsonify({'success': True, 'message': 'Rechazo logueado correctamente.'})

@compras_bp.route('/compras/api/reposicion/generar_np', methods=['POST'])
@login_required
@permission_required('compras.gestionar_reposicion')
@atomic_transaction('COMPRAS')
async def api_reposicion_generar_np():
    """Crea una Solicitud de Reposición formal a partir de una sugerencia del dashboard."""
    data = (await request.json) or {}
    articulo_id = data.get('articulo_id')
    cantidad = data.get('cantidad')
    
    if not articulo_id or not cantidad:
        return jsonify({'success': False, 'message': 'Datos incompletos.'}), 400
        
    ent_id = g.user['enterprise_id']
    uid = g.user['id']
    
    async with get_db_cursor() as cursor:
        # 1. Crear la cabecera de la solicitud (Nota de Pedido interna)
        await cursor.execute("""
            INSERT INTO cmp_solicitudes_reposicion (enterprise_id, fecha, solicitante_id, estado, prioridad, observaciones)
            VALUES (%s, NOW(), %s, 'PENDIENTE_AJUSTE', 2, 'Generado desde Dashboard de Reposición')
        """, (ent_id, uid))
        solicitud_id = cursor.lastrowid
        
        # 2. Insertar el detalle
        await cursor.execute("""
            INSERT INTO cmp_detalles_solicitud (enterprise_id, solicitud_id, articulo_id, cantidad_sugerida, user_id)
            VALUES (%s, %s, %s, %s, %s)
        """, (ent_id, solicitud_id, articulo_id, cantidad, uid))
        
    return jsonify({
        'success': True, 
        'message': f'Nota de Pedido #{solicitud_id} creada exitosamente.',
        'solicitud_id': solicitud_id
    })


async def _generar_asiento_contable_compra(cursor, comprobante_id, enterprise_id, user_id=None, items=None):

    """
    REFACTORIZADO: En lugar de generar el asiento contable inmediatamente,
    este proceso ahora se integra con el flujo de aprobación de costos.
    Si hay ítems, se generan propuestas de precios. El asiento contable
    debería generarse cuando el Gerente de Costos apruebe la operación.
    """
    from pricing.service import PricingService
    try:
        if items:
            # Transformar formato para el servicio de pricing
            items_pricing = []
            for it in items:
                items_pricing.append({
                    'articulo_id': it['articulo_id'],
                    'costo_calculado': float(it['precio_unitario']),
                    'precio_sugerido': float(it['precio_unitario']) * 1.3 # Sugerido base 30%
                })
            
            # Generar propuestas pendientes
            await PricingService.generar_propuestas_desde_costo(
                enterprise_id, 'FACTURA_LOCAL', comprobante_id, items_pricing, user_id
            )
            print(f"[COMPRAS] Propuestas de precios generadas para comprobante {comprobante_id}")
        
        # Por ahora, devolvemos None para no generar el asiento contable directo
        # La generación del asiento se moverá al proceso de aprobación de precios/costos
        return None
        
    except Exception as e:
        print(f"[COMPRAS] ERROR al procesar costeo/asiento: {e}")
        return None

@compras_bp.route('/compras/solicitudes', methods=['GET'])
@login_required
@permission_required('compras.gestionar_reposicion')
async def solicitudes_lista():
    """Listado de Solicitudes de Reposición (NPs) generadas."""
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT cmp_solicitudes_reposicion.*, sys_users.username as solicitante_nombre,
                   (SELECT COUNT(*) FROM cmp_detalles_solicitud WHERE solicitud_id = cmp_solicitudes_reposicion.id) as items_cnt
            FROM cmp_solicitudes_reposicion
            JOIN sys_users ON cmp_solicitudes_reposicion.solicitante_id = sys_users.id
            WHERE cmp_solicitudes_reposicion.enterprise_id = %s
            ORDER BY cmp_solicitudes_reposicion.fecha DESC
        """, (ent_id,))
        solicitudes = await cursor.fetchall()
    return await render_template('compras/solicitudes_lista.html', solicitudes=solicitudes)

@compras_bp.route('/compras/solicitud/<int:id>/cotizar', methods=['POST'])
@login_required
@permission_required('compras.solicitar_cotizacion')
@atomic_transaction('COMPRAS')
async def api_solicitud_cotizar(id):
    """Convierte una Solicitud (NP) en una Solicitud de Cotización (RFQ) para un proveedor."""
    data = (await request.json) or {}
    proveedor_id = data.get('proveedor_id')
    fecha_vencimiento = data.get('fecha_vencimiento') # La fecha limite definida por el comprador
    
    if not proveedor_id or not fecha_vencimiento:
        return jsonify({'success': False, 'message': 'Debe seleccionar un proveedor y definir una fecha límite.'}), 400
        
    ent_id = g.user['enterprise_id']
    uid = g.user['id']
    
    async with get_db_cursor(dictionary=True) as cursor:
        # 1. Obtener items de la solicitud
        await cursor.execute("SELECT * FROM cmp_detalles_solicitud WHERE solicitud_id = %s AND enterprise_id = %s", (id, ent_id))
        items = await cursor.fetchall()
        if not items:
            return jsonify({'success': False, 'message': 'La solicitud no tiene ítems.'}), 400
            
        # 2. Crear la Cotización (RFQ)
        # Generar hash de seguridad para el Excel
        from services.quotation_mailer import QuotationMailer
        mailer = QuotationMailer(ent_id)
        
        await cursor.execute("SELECT codigo FROM erp_terceros WHERE id = %s", (proveedor_id,))
        prov = await cursor.fetchone()
        sec_hash = await mailer.generate_security_hash(prov['codigo'] if prov else f"PROV{proveedor_id}")
        
        await cursor.execute("""
            INSERT INTO cmp_cotizaciones (enterprise_id, proveedor_id, fecha_envio, fecha_vencimiento, estado, security_hash, user_id)
            VALUES (%s, %s, NOW(), %s, 'ENVIADA', %s, %s)
        """, (ent_id, proveedor_id, fecha_vencimiento, sec_hash, uid))
        cot_id = cursor.lastrowid
        
        # 3. Insertar los items en la cotización
        for it in items:
            await cursor.execute("""
                INSERT INTO cmp_items_cotizacion (enterprise_id, cotizacion_id, articulo_id, cantidad, user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (ent_id, cot_id, it['articulo_id'], it['cantidad_sugerida'], uid))
            
        # 4. Actualizar estado de la solicitud
        await cursor.execute("UPDATE cmp_solicitudes_reposicion SET estado = 'COTIZANDO' WHERE id = %s", (id,))

        from quart import session, g
        sid = getattr(g, 'sid', None) or session.get('session_id')
        # 5. Log de Auditoría
        await cursor.execute("""
            INSERT INTO sys_transaction_logs (enterprise_id, user_id, session_id, module, endpoint, request_method, status, severity, impact_category, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (ent_id, uid, sid, 'COMPRAS', f'/compras/solicitud/{id}/cotizar', 'POST', 
              'SUCCESS', 2, 'Operational', f"RFQ_GENERADA: Cotización #{cot_id} desde NP #{id}. Proveedor ID: {proveedor_id}. Límite: {fecha_vencimiento}"))
        
    return jsonify({
        'success': True, 
        'message': f'Solicitud de Cotización #{cot_id} generada y lista para envío. Fecha límite: {fecha_vencimiento}'
    })


async def _generar_asiento_orden_pago(cursor, orden_pago_id, enterprise_id, user_id=None):

    """Genera el asiento contable para la cancelación de pasivos (Órdenes de Pago) y emite pasivos fiscales por Retenciones."""
    try:
        # Recuperar OP y proveedor
        await cursor.execute("""
            SELECT fin_ordenes_pago.*, erp_terceros.nombre as proveedor_nombre 
            FROM fin_ordenes_pago 
            JOIN erp_terceros ON fin_ordenes_pago.tercero_id = erp_terceros.id 
            WHERE fin_ordenes_pago.id = %s AND fin_ordenes_pago.enterprise_id = %s
        """, (orden_pago_id, enterprise_id))
        op = await cursor.fetchone()
        if not op: return None

        # MAPEAR CUENTA DE PROVEEDOR, RETENCIONES Y ANTICIPOS
        await cursor.execute("SELECT id, codigo FROM cont_plan_cuentas WHERE enterprise_id = %s AND codigo IN ('2.1.01', '2.2.05', '1.3.05')", (enterprise_id,))
        cuentas = {row['codigo']: row['id'] for row in await cursor.fetchall()}
        
        # Validar 
        cta_proveedores = cuentas.get('2.1.01')
        cta_retenciones = cuentas.get('2.2.05')
        
        if not cta_proveedores:
            print("[COMPRAS] ERROR: Falta cuenta 2.1.01 (Proveedores) en el plan.")
            return None

        # Asignar próximo_nro
        await cursor.execute("SELECT COALESCE(MAX(numero_asiento), 0) + 1 as proximo FROM cont_asientos WHERE enterprise_id = %s", (enterprise_id,))
        row_n = await cursor.fetchone()
        proximo_nro_asiento = row_n['proximo'] if row_n else 1

        concepto = f"Orden de Pago a {op['proveedor_nombre'][:30]} OP-{op['numero']}"

        # CABECERA ASIENTO
        await cursor.execute("""
            INSERT INTO cont_asientos (enterprise_id, fecha, concepto, modulo_origen, comprobante_id, numero_asiento, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (enterprise_id, op['fecha'], concepto, 'PAGOS', orden_pago_id, proximo_nro_asiento, user_id))
        asiento_id = cursor.lastrowid

        # ALGEBRA DEL DEBE Y HABER (EGRESOS CANCELAN PASIVOS, AUMENTAN OTROS O BAJAN ACTIVOS)
        detalles_asiento = []
        
        # DEBE (Cargo): Baja de Pasivo con Proveedores por el total pagado a facturas
        # (Para Anticipos el monto iría a 1.3.05, pero asumiremos todo Proveedores si no diferecia total_facturas de anticipo por ahora)
        monto_facturas = float(op['importe_total'] or 0)
        detalles_asiento.append((cta_proveedores, monto_facturas, 0.0))
        
        # HABER (Descargo): Por retenciones emitidas (Nace un Pasivo fiscal distinto)
        monto_retenciones = float(op['importe_retenciones'] or 0)
        if monto_retenciones > 0 and cta_retenciones:
            detalles_asiento.append((cta_retenciones, 0.0, monto_retenciones))
            
        # HABER (Descargo): Medios de Pago usados (Caja, Banco, Cheques) extraídos del snapshot
        await cursor.execute("""
            SELECT cuenta_contable_snapshot_id, importe 
            FROM fin_ordenes_pago_medios 
            WHERE orden_pago_id = %s
        """, (orden_pago_id,))
        for mp in await cursor.fetchall():
            if mp['cuenta_contable_snapshot_id']:
                # El importe de caja/banco sale (Haber)
                detalles_asiento.append((mp['cuenta_contable_snapshot_id'], 0.0, float(mp['importe'])))
            else:
                print(f"[COMPRAS] Advertencia: Medio de pago en OP {orden_pago_id} no posee snapshot_id. No cierra asiento.")
                
        # INSERTAR DETALLES 
        for cta_id, debe, haber in detalles_asiento:
            if cta_id and (debe > 0 or haber > 0):
                await cursor.execute("""
                    INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, enterprise_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (asiento_id, cta_id, debe, haber, enterprise_id))

        return asiento_id
    except Exception as e:
        print(f"[COMPRAS] ERROR al generar asiento (Pagos): {e}")
        return None


@compras_bp.route('/compras/dashboard')
@login_required
@permission_required('view_compras')
async def dashboard():
    try:
        kpi = {
            'enviadas': 0, 'recibidas': 0, 'efectividad': 0, 
            'items_no_provistos': 0, 'valor_no_provisto': 0
        }
        
        async with get_db_cursor(dictionary=True) as cursor:
            # 1. Total Enviadas
            await cursor.execute("SELECT COUNT(*) as total FROM cmp_cotizaciones WHERE estado != 'BORRADOR' AND enterprise_id = %s", (g.user['enterprise_id'],))
            row = await cursor.fetchone()
            kpi['enviadas'] = row['total'] if row else 0
            
            # 2. Total Recibidas
            await cursor.execute("SELECT COUNT(*) as total FROM cmp_cotizaciones WHERE estado IN ('RECIBIDA_PARCIAL', 'RECIBIDA_TOTAL', 'CONFIRMADA') AND enterprise_id = %s", (g.user['enterprise_id'],))
            row = await cursor.fetchone()
            kpi['recibidas'] = row['total'] if row else 0
            
            # 3. Efectividad %
            if kpi['enviadas'] > 0:
                kpi['efectividad'] = round((kpi['recibidas'] / kpi['enviadas']) * 100, 1)
                
            # 4. Material No Provisto (Items en Cotizaciones Cerradas con cantidad_ofrecida=0 o NULL)
            # Asumiendo costo en stk_articulos.costo (o costo_reposicion, standard_cost...)
            # Si no existe costo, usar 0.
            await cursor.execute("""
                SELECT COUNT(*) as count, SUM(cmp_items_cotizacion.cantidad * COALESCE(stk_articulos.costo, 0)) as valor
                FROM cmp_items_cotizacion
                JOIN cmp_cotizaciones ON cmp_items_cotizacion.cotizacion_id = cmp_cotizaciones.id
                JOIN stk_articulos ON cmp_items_cotizacion.articulo_id = stk_articulos.id
                WHERE cmp_cotizaciones.estado IN ('RECIBIDA_TOTAL', 'CONFIRMADA') 
                  AND (cmp_items_cotizacion.cantidad_ofrecida IS NULL OR cmp_items_cotizacion.cantidad_ofrecida = 0)
                  AND cmp_cotizaciones.enterprise_id = %s AND cmp_items_cotizacion.enterprise_id = %s
            """, (g.user['enterprise_id'], g.user['enterprise_id']))
            row = await cursor.fetchone()
            if row:
                kpi['items_no_provistos'] = row['count']
                kpi['valor_no_provisto'] = row['valor'] if row['valor'] else 0
    
            # Load Recent Lists
            await cursor.execute("""
                SELECT cmp_cotizaciones.*, cmp_cotizaciones.fecha_envio as fecha, erp_terceros.nombre as razon_social, (SELECT COUNT(*) FROM cmp_items_cotizacion WHERE cotizacion_id=cmp_cotizaciones.id AND enterprise_id = %s) as items_cnt
                FROM cmp_cotizaciones
                JOIN erp_terceros ON cmp_cotizaciones.proveedor_id = erp_terceros.id
                WHERE cmp_cotizaciones.enterprise_id = %s
                ORDER BY cmp_cotizaciones.fecha_envio DESC LIMIT 10
            """, (g.user['enterprise_id'], g.user['enterprise_id']))
            cotizaciones = await cursor.fetchall()
            
            # Alert List (Unprovided)
            await cursor.execute("""
                SELECT cmp_items_cotizacion.*, stk_articulos.nombre as articulo, stk_articulos.codigo, cmp_cotizaciones.fecha_envio as fecha, erp_terceros.nombre as razon_social
                FROM cmp_items_cotizacion
                JOIN cmp_cotizaciones ON cmp_items_cotizacion.cotizacion_id = cmp_cotizaciones.id
                JOIN stk_articulos ON cmp_items_cotizacion.articulo_id = stk_articulos.id
                JOIN erp_terceros ON cmp_cotizaciones.proveedor_id = erp_terceros.id
                WHERE cmp_cotizaciones.estado IN ('RECIBIDA_TOTAL', 'CONFIRMADA') 
                AND (cmp_items_cotizacion.cantidad_ofrecida IS NULL OR cmp_items_cotizacion.cantidad_ofrecida = 0)
                AND cmp_cotizaciones.enterprise_id = %s AND cmp_items_cotizacion.enterprise_id = %s
                ORDER BY cmp_cotizaciones.fecha_envio DESC LIMIT 20
            """, (g.user['enterprise_id'], g.user['enterprise_id']))
            alertas = await cursor.fetchall()
    
        return await render_template('compras/dashboard.html', kpi=kpi, cotizaciones=cotizaciones, alertas=alertas)
    except Exception as e:
        import traceback
        traceback.print_exc()
        await flash(f"Error al cargar el dashboard de compras: {str(e)}", "danger")
        return redirect('/')

@compras_bp.route('/compras/comprobantes')
@login_required
@permission_required('view_proveedores')
async def comprobantes():
    """Listado completo de comprobantes de compra (Facturas, NC, ND)."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT erp_comprobantes.*, erp_terceros.nombre as proveedor_nombre, sys_tipos_comprobante.letra, sys_tipos_comprobante.descripcion as tipo_nombre
            FROM erp_comprobantes
            JOIN erp_terceros ON erp_comprobantes.tercero_id = erp_terceros.id
            JOIN sys_tipos_comprobante ON erp_comprobantes.tipo_comprobante = sys_tipos_comprobante.codigo
            WHERE erp_comprobantes.enterprise_id = %s AND erp_comprobantes.tipo_operacion = 'COMPRA'
            ORDER BY erp_comprobantes.fecha_emision DESC, erp_comprobantes.numero DESC
        """, (g.user['enterprise_id'],))
        lista = await cursor.fetchall()
    return await render_template('compras/comprobantes.html', comprobantes=lista)

@compras_bp.route('/compras/cotizaciones')
@login_required
@permission_required('compras.ver_reportes')
async def cotizaciones():
    estado = request.args.get('estado')
    async with get_db_cursor(dictionary=True) as cursor:
        query = """
            SELECT cmp_cotizaciones.*, erp_terceros.nombre as razon_social, 
                   (SELECT COUNT(*) FROM cmp_items_cotizacion WHERE cotizacion_id=cmp_cotizaciones.id AND enterprise_id = %s) as items_cnt
            FROM cmp_cotizaciones
            JOIN erp_terceros ON cmp_cotizaciones.proveedor_id = erp_terceros.id
            WHERE cmp_cotizaciones.enterprise_id = %s
        """
        params = [g.user['enterprise_id'], g.user['enterprise_id']]
        if estado:
            query += " AND cmp_cotizaciones.estado = %s"
            params.append(estado)
        query += " ORDER BY cmp_cotizaciones.fecha_envio DESC"
        await cursor.execute(query, tuple(params))
        rows = await cursor.fetchall()
    return await render_template('compras/cotizaciones_lista.html', cotizaciones=rows)

@compras_bp.route('/compras/cotizacion/<int:id>', methods=['GET', 'POST'])
@login_required
@permission_required('compras.ver_reportes')
async def cotizacion_detalle(id):
    async with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            # Manual update by admin (e.g. if supplier sent a PDF/email)
            for key, value in (await request.form).items():
                if key.startswith('cant_'):
                    item_id = key.split('_')[1]
                    cant = float(value) if value else 0
                    price = float((await request.form).get(f'price_{item_id}', 0))
                    
                    await cursor.execute("""
                        UPDATE cmp_items_cotizacion 
                        SET cantidad_ofrecida = %s, precio_cotizado = %s 
                        WHERE id = %s AND cotizacion_id = %s AND enterprise_id = %s
                    """, (cant, price, item_id, id, g.user['enterprise_id']))
            
            # Update quotation status to RESPONDIDA if it was ENVIADA
            await cursor.execute("UPDATE cmp_cotizaciones SET estado = 'RESPONDIDA' WHERE id = %s AND enterprise_id = %s AND estado = 'ENVIADA'", (id, g.user['enterprise_id']))
            await flash("Cotización actualizada manualmente.", "success")
            return redirect(url_for('compras.cotizacion_detalle', id=id))

        await cursor.execute("""
            SELECT cmp_cotizaciones.*, erp_terceros.nombre as razon_social, erp_terceros.email as proveedor_email, erp_terceros.telefono as proveedor_tel
            FROM cmp_cotizaciones
            JOIN erp_terceros ON cmp_cotizaciones.proveedor_id = erp_terceros.id
            WHERE cmp_cotizaciones.id = %s AND cmp_cotizaciones.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        cot = await cursor.fetchone()
        
        if not cot:
            await flash("Cotización no encontrada.", "danger")
            return redirect(url_for('compras.dashboard'))

        await cursor.execute("""
            SELECT cmp_items_cotizacion.*, stk_articulos.nombre as articulo_nombre, stk_articulos.codigo as articulo_codigo
            FROM cmp_items_cotizacion
            JOIN stk_articulos ON cmp_items_cotizacion.articulo_id = stk_articulos.id
            WHERE cmp_items_cotizacion.cotizacion_id = %s AND cmp_items_cotizacion.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        items = await cursor.fetchall()
        
    return await render_template('compras/cotizacion_detalle.html', cot=cot, items=items)

@compras_bp.route('/compras/alertas-detalle')
@login_required
async def alertas_detalle():
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT cmp_items_cotizacion.*, stk_articulos.nombre as articulo, stk_articulos.codigo, cmp_cotizaciones.fecha_envio as fecha, erp_terceros.nombre as razon_social, cmp_cotizaciones.id as cotizacion_id
            FROM cmp_items_cotizacion
            JOIN cmp_cotizaciones ON cmp_items_cotizacion.cotizacion_id = cmp_cotizaciones.id
            JOIN stk_articulos ON cmp_items_cotizacion.articulo_id = stk_articulos.id
            JOIN erp_terceros ON cmp_cotizaciones.proveedor_id = erp_terceros.id
            WHERE cmp_cotizaciones.estado IN ('RECIBIDA_TOTAL', 'CONFIRMADA') 
              AND (cmp_items_cotizacion.cantidad_ofrecida IS NULL OR cmp_items_cotizacion.cantidad_ofrecida = 0)
              AND cmp_cotizaciones.enterprise_id = %s AND cmp_items_cotizacion.enterprise_id = %s
            ORDER BY cmp_cotizaciones.fecha_envio DESC
        """, (g.user['enterprise_id'], g.user['enterprise_id']))
        alertas = await cursor.fetchall()
    return await render_template('compras/alertas_detalle.html', alertas=alertas)

@compras_bp.route('/compras/proveedores/nuevo', methods=['GET', 'POST'])
@login_required
@atomic_transaction('compras')
async def nuevo_proveedor():
    if request.method == 'POST':
        codigo = (await request.form).get('codigo', '')
        nombre = (await request.form)['nombre']
        cuit = (await request.form)['cuit']
        email = (await request.form)['email']
        tipo = (await request.form)['tipo_responsable']
        observaciones = (await request.form).get('observaciones', '')
        
        from services.validation_service import validar_cuit, format_cuit
        if not validar_cuit(cuit):
            await flash("Error: El CUIT ingresado no es válido.", "danger")
            return await render_template('compras/proveedor_form.html')
        
        cuit = format_cuit(cuit)
        ent_id = g.user['enterprise_id']

        # --- CIRUGÍA DE PRECISIÓN: SCANNER DE SEGURIDAD AFIP ---
        # 1. FEDummy: Verificación de Túneles
        scout = await AfipService.fe_dummy()
        if not scout['success']:
            await AfipService.registrar_bitacora(ent_id, "TUNEL_BLOQUEADO", "WARNING", f"FEDummy interceptado: {scout['error']}")
            await flash("⚠️ Los túneles de AFIP parecen estar bloqueados por Sentinels. Se procederá con validación manual.", "info")
        
        # 2. Scanner APOC: Detección de Traidores
        apoc = await AfipService.consultar_base_apoc(ent_id, cuit)
        if apoc.get('es_apocrifo'):
            await AfipService.registrar_bitacora(ent_id, "INTRUSO_APOCRIFO", "CRITICAL", f"Intento de registro de CUIT apócrifo: {cuit}")
            await flash(f"🚨 PROTOCOLO DE DEFENSA: El CUIT {cuit} figura en la base APOC de AFIP. No se permite su ingreso a la Matrix.", "danger")
            return await render_template('compras/proveedor_form.html')

        # 3. Niobe (A10) & wconsucuit (A13): Identificación de Sujetos
        niobe = await AfipService.consultar_datos_a10(ent_id, cuit)
        if niobe['success']:
            official_name = niobe.get('nombre')
            if official_name and official_name != 'Desconocido' and official_name.upper() != nombre.upper():
                await flash(f"🔍 Discrepancia detectada: El nombre oficial en AFIP es '{official_name}'. Verifique antes de continuar.", "warning")
            await AfipService.registrar_bitacora(ent_id, "ID_VALIDADA_A10", "INFO", f"Niobe validó al proveedor {cuit}")
        else:
            # Reintento con wconsucuit si A10 falla
            scout_a13 = await AfipService.consultar_cuit(ent_id, cuit)
            if scout_a13['success']:
                await AfipService.registrar_bitacora(ent_id, "ID_VALIDADA_A13", "INFO", f"wconsucuit validó al proveedor {cuit}")
            else:
                await AfipService.registrar_bitacora(ent_id, "SUJETO_NO_IDENTIFICADO", "ALERT", f"No se pudo validar identidad de {cuit} via AFIP")
                await flash("⚠️ Advertencia: No se pudo verificar la identidad del proveedor en los registros de AFIP.", "warning")
        try:
            async with get_db_cursor(dictionary=True) as cursor:
                await cursor.execute("SELECT id FROM erp_terceros WHERE cuit = %s AND enterprise_id = %s", (cuit, g.user['enterprise_id']))
                if await cursor.fetchone():
                    await flash("Error: Ya existe un proveedor con ese CUIT ("+cuit+").", "danger")
                else:
                    # Generar código si no se proveyó
                    if not codigo:
                        codigo = await TerceroService.generar_siguiente_codigo(g.user['enterprise_id'], 'PRO')

                    await cursor.execute("""
                        INSERT INTO erp_terceros (enterprise_id, codigo, nombre, cuit, email, observaciones, es_cliente, es_proveedor, tipo_responsable, naturaleza)
                        VALUES (%s, %s, %s, %s, %s, %s, 0, 1, %s, 'PRO')
                    """, (g.user['enterprise_id'], codigo, nombre, cuit, email, observaciones, tipo))
                    await flash(f"Proveedor registrado exitosamente con el número {codigo}. Complete los detalles ahora.", "success")
                    await cursor.execute("SELECT LAST_INSERT_ID() as last_id")
                    new_id = await cursor.fetchone()['last_id']
                    return redirect(url_for('compras.perfil_proveedor', id=new_id))
        except Exception as e:
            err_msg = str(e)
            await AfipService.registrar_bitacora(ent_id, "FALLO_SISTEMA_COMPRAS", "ERROR", f"Error en registro de proveedor: {err_msg}")
            await flash(f"Error: {err_msg}", "danger")
            
    return await render_template('compras/proveedor_form.html')

@compras_bp.route('/compras/proveedores/editar/<int:id>', methods=['POST'])
@login_required
async def editar_proveedor(id):
    nombre = (await request.form)['nombre']
    cuit = (await request.form)['cuit']
    email = (await request.form)['email']
    tipo = (await request.form)['tipo_responsable']
    observaciones = (await request.form).get('observaciones', '')
    telefono = (await request.form).get('telefono', '')
    codigo = (await request.form).get('codigo', '')

    from services.validation_service import validar_cuit, format_cuit
    if not validar_cuit(cuit):
        await flash("Error: El CUIT ingresado no es válido.", "danger")
        return redirect(url_for('compras.perfil_proveedor', id=id))
    
    cuit = format_cuit(cuit)

    try:
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("""
                UPDATE erp_terceros 
                SET codigo=%s, nombre=%s, cuit=%s, email=%s, tipo_responsable=%s, observaciones=%s, telefono=%s, user_id_update=%s
                WHERE id=%s AND enterprise_id=%s
            """, (codigo, nombre, cuit, email, tipo, observaciones, telefono, g.user['id'], id, g.user['enterprise_id']))
            await flash("Datos del proveedor actualizados.", "success")
    except Exception as e:
        await flash(f"Error al actualizar: {str(e)}", "danger")
    
    return redirect(url_for('compras.perfil_proveedor', id=id))

@compras_bp.route('/compras/proveedores')
@login_required
async def proveedores():
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT erp_terceros.*, erp_direcciones.calle, erp_direcciones.numero, erp_direcciones.localidad, erp_direcciones.provincia 
            FROM erp_terceros
            LEFT JOIN erp_direcciones ON erp_terceros.id = erp_direcciones.tercero_id AND erp_direcciones.es_fiscal = 1
            WHERE (erp_terceros.enterprise_id = %s OR erp_terceros.enterprise_id = 0) AND erp_terceros.es_proveedor = 1
            GROUP BY erp_terceros.id
            ORDER BY erp_terceros.nombre
        """, (g.user['enterprise_id'],))
        proveedores = await cursor.fetchall()
    return await render_template('compras/proveedores.html', proveedores=proveedores)

@compras_bp.route('/compras/proveedores/perfil/<int:id>')
@login_required
async def perfil_proveedor(id):
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            # Step 1: Basic Info
            await cursor.execute("SELECT * FROM erp_terceros WHERE id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            proveedor = await cursor.fetchone()
            if not proveedor:
                await flash("Proveedor no encontrado.", "danger")
                return redirect(url_for('compras.proveedores'))
                
            # Step 2: Direcciones
            await cursor.execute("SELECT * FROM erp_direcciones WHERE tercero_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            direcciones = await cursor.fetchall()
            
            # Step 3: Contactos
            await cursor.execute("""
                SELECT erp_contactos.*, erp_puestos.nombre as puesto_nombre 
                FROM erp_contactos
                LEFT JOIN erp_puestos ON erp_contactos.puesto_id = erp_puestos.id
                WHERE erp_contactos.tercero_id = %s AND erp_contactos.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            contactos = await cursor.fetchall()
            
            # Step 4: Datos Fiscales
            await cursor.execute("SELECT * FROM erp_datos_fiscales WHERE tercero_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            fiscales = await cursor.fetchall()
            
            # Step 5: Coeficientes CM05
            await cursor.execute("""
                SELECT erp_terceros_cm05.*, sys_provincias.nombre as provincia_nombre
                FROM erp_terceros_cm05
                LEFT JOIN sys_provincias ON BINARY erp_terceros_cm05.jurisdiccion_code = BINARY LPAD(sys_provincias.id, 3, '0')
                WHERE erp_terceros_cm05.tercero_id = %s AND erp_terceros_cm05.enterprise_id = %s
                ORDER BY erp_terceros_cm05.periodo_anio DESC, erp_terceros_cm05.jurisdiccion_code ASC
            """, (id, g.user['enterprise_id']))
            coeficientes_cm = await cursor.fetchall()
            
            # Fallback
            for c in coeficientes_cm:
                if not c.get('provincia_nombre'):
                    c['provincia_nombre'] = f"Jurisdicción {c['jurisdiccion_code']}"
                    
            # Step 6: Provincias
            provincias = await GeorefService.get_provincias()
            
        return await render_template('compras/perfil_proveedor.html', proveedor=proveedor, direcciones=direcciones, contactos=contactos, fiscales=fiscales, provincias=provincias, coeficientes_cm=coeficientes_cm)
    except Exception as e:
        import traceback
        print(f"ERROR IN PERFIL_PROVEEDOR: {e}")
        traceback.print_exc()
        raise e

@compras_bp.route('/compras/api/proveedor/<int:id>/audit', methods=['POST'])
@login_required
@permission_required('admin_services')
async def api_proveedor_audit(id):
    """
    Ejecuta un escaneo profundo (Deep Scan) de un proveedor existente.
    Invoca a Niobe (A10), wconsucuit (A13) y el Scanner APOC.
    """
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT cuit, nombre FROM erp_terceros WHERE id = %s AND enterprise_id = %s", (id, ent_id))
        prov = await cursor.fetchone()
    
    if not prov:
        return jsonify({'success': False, 'error': 'Proveedor no encontrado.'}), 404
        
    cuit = prov['cuit']
    results = {
        'apoc': await AfipService.consultar_base_apoc(ent_id, cuit),
        'a10': await AfipService.consultar_datos_a10(ent_id, cuit),
    }
    
    status = "CLEAN"
    if results['apoc'].get('es_apocrifo'):
        status = "TRAITOR"
        await AfipService.registrar_bitacora(ent_id, "AUDITORIA_VULNERABILIDAD", "CRITICAL", f"Auditoría reveló que {prov['nombre']} ({cuit}) es APÓCRIFO.")
    else:
        await AfipService.registrar_bitacora(ent_id, "AUDITORIA_PROVEEDOR", "INFO", f"Auditoría profunda completada para {prov['nombre']} ({cuit}).")

    return jsonify({
        'success': True,
        'status': status,
        'results': {
            'apoc': results['apoc'].get('mensaje'),
            'a10': results['a10'].get('nombre', 'No encontrado')
        }
    })

@compras_bp.route('/compras/proveedores/toggle-convenio/<int:id>', methods=['POST'])
@login_required
async def toggle_convenio(id):
    es_convenio = 1 if 'es_convenio' in (await request.form) else 0
    async with get_db_cursor() as cursor:
        await cursor.execute("UPDATE erp_terceros SET es_convenio_multilateral = %s WHERE id = %s AND enterprise_id = %s", (es_convenio, id, g.user['enterprise_id']))
    await flash("Configuración de convenio multilateral actualizada.", "success")
    return redirect(url_for('compras.perfil_proveedor', id=id))

from services.cm05_service import CM05Service

@compras_bp.route('/compras/proveedores/agregar-cm05/<int:id>', methods=['POST'])
@login_required
async def agregar_cm05(id):
    jurisdiccion = (await request.form)['jurisdiccion_code']
    periodo_anio = (await request.form)['periodo_anio']
    coeficiente = (await request.form)['coeficiente']

    try:
        await CM05Service.upsert_coeficiente(g.user['enterprise_id'], id, jurisdiccion, periodo_anio, coeficiente, g.user['id'])
        await flash("Coeficiente guardado correctamente.", "success")
    except Exception as e:
        await flash(f"Error al guardar coeficiente: {e}", "danger")
    
    return redirect(url_for('compras.perfil_proveedor', id=id))

import os
from werkzeug.utils import secure_filename

@compras_bp.route('/compras/proveedores/upload-cm05/<int:id>', methods=['POST'])
@login_required
async def upload_cm05(id):
    if 'archivo_cm05' not in (await request.files):
        await flash('No se seleccionó ningún archivo.', 'warning')
        return redirect(url_for('compras.perfil_proveedor', id=id))
        
    file = (await request.files)['archivo_cm05']
    if file.filename == '':
        await flash('No se seleccionó ningún archivo.', 'warning')
        return redirect(url_for('compras.perfil_proveedor', id=id))
        
    if file and file.filename.lower().endswith('.pdf'):
        filename = secure_filename(f"CM05_PRV_{g.user['enterprise_id']}_{id}_{file.filename}")
        upload_folder = os.path.join(os.getcwd(), 'static', 'uploads', 'cm05')
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, filename)
        await file.save(file_path)
        
        # Guardar solo la ruta relativa
        rel_path = f"uploads/cm05/{filename}"
        
        async with get_db_cursor() as cursor:
            try:
                await cursor.execute("UPDATE erp_terceros SET archivo_cm05_path = %s WHERE id = %s", (rel_path, id))
            except BaseException:
                # Add column if it doesn't exist
                await cursor.execute("ALTER TABLE erp_terceros ADD COLUMN archivo_cm05_path VARCHAR(255) NULL")
                await cursor.execute("UPDATE erp_terceros SET archivo_cm05_path = %s WHERE id = %s", (rel_path, id))
        
        await flash("Archivo subido correctamente.", "success")
    else:
        await flash("Formato de archivo inválido. Solo PDF.", "danger")
        
    return redirect(url_for('compras.perfil_proveedor', id=id))

@compras_bp.route('/compras/proveedores/agregar-direccion/<int:id>', methods=['POST'])
@login_required
async def agregar_direccion(id):
    item_id = (await request.form).get('item_id')
    etiqueta = (await request.form)['etiqueta']
    calle = (await request.form)['calle']
    numero = (await request.form)['numero']
    piso = (await request.form).get('piso', '')
    depto = (await request.form).get('depto', '')
    localidad = (await request.form)['localidad']
    provincia = (await request.form)['provincia']
    cp = (await request.form)['cod_postal']
    es_fiscal = 1 if 'es_fiscal' in (await request.form) else 0
    es_entrega = 1 if 'es_entrega' in (await request.form) else 0
    async with get_db_cursor(dictionary=True) as cursor:
        if item_id:
            await cursor.execute("""
                UPDATE erp_direcciones 
                SET etiqueta=%s, calle=%s, numero=%s, piso=%s, depto=%s, localidad=%s, provincia=%s, cod_postal=%s, es_fiscal=%s, es_entrega=%s, user_id_update=%s
                WHERE id=%s AND tercero_id=%s AND enterprise_id=%s
            """, (etiqueta, calle, numero, piso, depto, localidad, provincia, cp, es_fiscal, es_entrega, g.user['id'], item_id, id, g.user['enterprise_id']))
            await flash("Dirección actualizada.", "success")
        else:
            await cursor.execute("""
                INSERT INTO erp_direcciones (enterprise_id, tercero_id, etiqueta, calle, numero, piso, depto, localidad, provincia, cod_postal, es_fiscal, es_entrega, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (g.user['enterprise_id'], id, etiqueta, calle, numero, piso, depto, localidad, provincia, cp, es_fiscal, es_entrega, g.user['id']))
            await flash("Dirección agregada.", "success")
    
    return redirect(url_for('compras.perfil_proveedor', id=id))

@compras_bp.route('/compras/proveedores/agregar-contacto/<int:id>', methods=['POST'])
@login_required
async def agregar_contacto(id):
    item_id = (await request.form).get('item_id')
    nombre = (await request.form)['nombre']
    puesto = (await request.form)['puesto']
    tipo = (await request.form)['tipo_contacto']
    telefono = (await request.form)['telefono']
    email = (await request.form)['email']

    async with get_db_cursor(dictionary=True) as cursor:
        # Resolver puesto a ID si existe
        await cursor.execute("SELECT id FROM erp_puestos WHERE nombre = %s AND enterprise_id = %s LIMIT 1", (puesto, g.user['enterprise_id']))
        puesto_row = await cursor.fetchone()
        puesto_id = puesto_row['id'] if puesto_row else None

        if item_id:
            await cursor.execute("""
                UPDATE erp_contactos SET nombre=%s, puesto_id=%s, tipo_contacto=%s, telefono=%s, email=%s
                WHERE id=%s AND tercero_id=%s AND enterprise_id=%s
            """, (nombre, puesto_id, tipo, telefono, email, item_id, id, g.user['enterprise_id']))
            await flash("Contacto actualizado.", "success")
        else:
            await cursor.execute("""
                INSERT INTO erp_contactos (enterprise_id, tercero_id, nombre, puesto_id, tipo_contacto, telefono, email)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (g.user['enterprise_id'], id, nombre, puesto_id, tipo, telefono, email))
            await flash("Contacto agregado.", "success")
    
    return redirect(url_for('compras.perfil_proveedor', id=id))

@compras_bp.route('/compras/proveedores/agregar-fiscal/<int:id>', methods=['POST'])
@login_required
async def agregar_fiscal(id):
    item_id = (await request.form).get('item_id')
    impuesto = (await request.form)['impuesto']
    jurisdiccion = (await request.form)['jurisdiccion']
    condicion = (await request.form)['condicion']
    inscripcion = (await request.form)['numero_inscripcion']
    alicuota = (await request.form).get('alicuota', 0)

    async with get_db_cursor(dictionary=True) as cursor:
        # Validar alícuota numérica
        try: alicuota = float(alicuota)
        except: alicuota = 0

        if item_id:
            await cursor.execute("""
                UPDATE erp_datos_fiscales SET impuesto=%s, jurisdiccion=%s, condicion=%s, numero_inscripcion=%s, alicuota=%s
                WHERE id=%s AND tercero_id=%s AND enterprise_id=%s
            """, (impuesto, jurisdiccion, condicion, inscripcion, alicuota, item_id, id, g.user['enterprise_id']))
            await flash("Dato fiscal actualizado.", "success")
        else:
            await cursor.execute("""
                INSERT INTO erp_datos_fiscales (enterprise_id, tercero_id, impuesto, jurisdiccion, condicion, numero_inscripcion, alicuota)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (g.user['enterprise_id'], id, impuesto, jurisdiccion, condicion, inscripcion, alicuota))
            await flash("Dato fiscal agregado.", "success")
    
    return redirect(url_for('compras.perfil_proveedor', id=id))

@compras_bp.route('/compras/proveedores/eliminar-detalle/<string:tabla>/<int:item_id>/<int:id>')
@login_required
async def eliminar_detalle(tabla, item_id, id):
    # Validar que la tabla sea una de las permitidas para evitar inyección
    tablas_permitidas = ['erp_direcciones', 'erp_contactos', 'erp_datos_fiscales', 'erp_terceros_cm05']
    if tabla not in tablas_permitidas:
        await flash("Operación no permitida.", "danger")
        return redirect(url_for('compras.perfil_proveedor', id=id))

    if tabla == 'erp_terceros_cm05':
        from services.cm05_service import CM05Service
        await CM05Service.delete_coeficiente(g.user['enterprise_id'], item_id, g.user['id'])
    else:
        async with get_db_cursor(dictionary=True) as cursor:
            # Verificar que el item pertenezca al tercero y que el tercero sea de la empresa correcta
            await cursor.execute(f"DELETE FROM {tabla} WHERE id = %s AND tercero_id = %s AND enterprise_id = %s", (item_id, id, g.user['enterprise_id']))
    await flash("Registro eliminado.", "info")
    return redirect(url_for('compras.perfil_proveedor', id=id))

# --- GENERAR PO DESDE COTIZACION ---

@compras_bp.route('/compras/cotizacion/<int:id>/generar_po', methods=['POST'])
@login_required
@atomic_transaction('compras')
async def generar_po(id):
    """Crea la Orden de Compra a partir de una Cotización RESPONDIDA y notifica al proveedor."""
    async with get_db_cursor(dictionary=True) as cursor:
        # Verificar que la cotización pertenece a la empresa y está en estado correcto
        await cursor.execute(
            "SELECT id, estado, proveedor_id FROM cmp_cotizaciones WHERE id = %s AND enterprise_id = %s",
            (id, g.user['enterprise_id'])
        )
        cot = await cursor.fetchone()

    if not cot:
        await flash("Cotización no encontrada.", "danger")
        return redirect(url_for('compras.cotizaciones'))

    if cot['estado'] not in ('RESPONDIDA', 'RECIBIDA_PARCIAL', 'RECIBIDA_TOTAL'):
        await flash(f"La cotización debe estar en estado RESPONDIDA para generar una PO. Estado actual: {cot['estado']}", "warning")
        return redirect(url_for('compras.cotizacion_detalle', id=id))

    try:
        async with get_db_cursor(dictionary=True) as cursor:
            mailer = PurchaseOrderMailer(g.user['enterprise_id'])
            po_data, error = await mailer.create_order_from_quotation(id, existing_cursor=cursor)

            if error:
                await flash(f"Error al crear la PO: {error}", "danger")
                return redirect(url_for('compras.cotizacion_detalle', id=id))

            po_id   = po_data['po_id']
            po_hash = po_data['hash']
            items   = po_data['items']

        # Intentar enviar email con Excel adjunto
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("SELECT nombre, email FROM erp_terceros WHERE id = %s", (cot['proveedor_id'],))
            prov = await cursor.fetchone()
            await cursor.execute("SELECT nombre FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
            empresa = await cursor.fetchone()

        email_ok = False
        if prov and prov.get('email') and items:
            excel_path = await mailer.generate_excel_po(po_id, prov['nombre'], items, po_hash)
            empresa_nombre = empresa['nombre'] if empresa else ''
            email_ok = await mailer.send_po_email(prov['email'], po_id, po_hash, excel_path, empresa_nombre)

        # Marcar cotización como CONFIRMADA
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute(
                "UPDATE cmp_cotizaciones SET estado = 'CONFIRMADA' WHERE id = %s AND enterprise_id = %s",
                (id, g.user['enterprise_id'])
            )

        msg_email = f" Email enviado a {prov['email']}." if email_ok else " (Email no enviado — revisar configuración de correo.)"
        await flash(f"✅ Orden de Compra #{po_id} creada exitosamente y enviada a aprobación.{msg_email}", "success")
        return redirect(url_for('compras.aprobar_po_detalle', id=po_id))

    except Exception as e:
        await flash(f"Error inesperado al generar la PO: {str(e)}", "danger")
        return redirect(url_for('compras.cotizacion_detalle', id=id))


# --- WORKFLOW DE APROBACION DE PO ---

@compras_bp.route('/compras/ordenes', endpoint='ordenes')
@login_required
async def ordenes():
    """Listado completo de Órdenes de Compra (Histórico)."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT o.*, p.nombre as proveedor_nombre, p.codigo as proveedor_codigo 
            FROM cmp_ordenes_compra o
            JOIN erp_terceros p ON o.proveedor_id = p.id
            WHERE o.enterprise_id = %s
            ORDER BY o.fecha_emision DESC
        """, (g.user['enterprise_id'],))
        ordenes = await cursor.fetchall()
    return await render_template('compras/ordenes_lista.html', ordenes=ordenes)


@compras_bp.route('/compras/aprobaciones')
@login_required
@permission_required('compras.aprobar_po')
async def aprobaciones():
    """Listado de POs esperando aprobación del Gerente de Compras."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT o.*, p.nombre as proveedor_nombre, p.codigo as proveedor_codigo 
            FROM cmp_ordenes_compra o
            JOIN erp_terceros p ON o.proveedor_id = p.id
            WHERE o.estado = 'PENDIENTE_APROBACION_COMPRAS' AND o.enterprise_id = %s
            ORDER BY o.fecha_emision DESC
        """, (g.user['enterprise_id'],))
        ordenes = await cursor.fetchall()
    return await render_template('compras/aprobaciones.html', ordenes=ordenes)

# --- NUEVA ORDEN (MANUAL) ---

@compras_bp.route('/compras/orden_nueva', methods=['GET', 'POST'])
@login_required
@atomic_transaction('compras')
async def orden_nueva():
    if request.method == 'POST':
        prov_id = (await request.form).get('proveedor_id')
        fecha_emision = (await request.form).get('fecha_emision')
        obs = (await request.form).get('observaciones')
        cc_id = (await request.form).get('centro_costo_id')
        
        if not prov_id or not cc_id:
            await flash("Debe seleccionar un proveedor y un Centro de Costos.", "warning")
            return redirect(url_for('compras.orden_nueva'))

        try:
            async with get_db_cursor() as cursor:
                # Generar hash unico para link externo (opcional)
                po_hash = f"PO_{g.user['enterprise_id']}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
                
                await cursor.execute("""
                    INSERT INTO cmp_ordenes_compra 
                    (enterprise_id, proveedor_id, centro_costo_id, estado, fecha_emision, observaciones, security_hash, total_estimado, user_id)
                    VALUES (%s, %s, %s, 'PENDIENTE_APROBACION_COMPRAS', %s, %s, %s, 0, %s)
                """, (g.user['enterprise_id'], prov_id, cc_id, fecha_emision, obs, po_hash, g.user['id']))
                po_id = cursor.lastrowid
                
            await flash(f"Orden de Compra #{po_id} creada. Ahora agregue los ítems.", "success")
            return redirect(url_for('compras.aprobar_po_detalle', id=po_id))
            
        except Exception as e:
            await flash(f"Error creando orden: {e}", "danger")
            return redirect(url_for('compras.orden_nueva'))

    # GET: Mostrar formulario
    hoy = datetime.date.today().strftime('%Y-%m-%d')
    proveedores = await TerceroService.get_proveedores_for_selector(g.user['enterprise_id'])
    centros_costo = await BudgetService.get_cost_centers(g.user['enterprise_id'])
        
    return await render_template('compras/orden_nueva.html', proveedores=proveedores, centros_costo=centros_costo, hoy=hoy)

@compras_bp.route('/compras/orden/<int:id>/agregar_item', methods=['POST'])
@login_required
async def agregar_item_po(id):
    """Agrega un ítem a una PO existente en estado borrador/pendiente."""
    articulo_id = (await request.form).get('articulo_id')
    cantidad = (await request.form).get('cantidad', 1)
    
    if not articulo_id:
        await flash("Seleccione un artículo.", "warning")
        return redirect(url_for('compras.aprobar_po_detalle', id=id))

    async with get_db_cursor(dictionary=True) as cursor:
        # Verificar estado PO
        await cursor.execute("SELECT estado FROM cmp_ordenes_compra WHERE id=%s AND enterprise_id=%s", (id, g.user['enterprise_id']))
        po = await cursor.fetchone()
        if not po or po['estado'] not in ('PENDIENTE_APROBACION_COMPRAS', 'RECHAZADA_TESORERIA', 'RECHAZADA_COMPRAS'):
             await flash("No se pueden agregar ítems a esta PO en su estado actual.", "warning")
             return redirect(url_for('compras.aprobar_po_detalle', id=id))

        # Obtener costo estandar del articulo como precio inicial
        # Prioridad: 1. Costo Reposición, 2. Costo Base (Lista), 3. Costo Promedio
        await cursor.execute("SELECT costo_reposicion, costo, costo_promedio FROM stk_articulos WHERE id=%s", (articulo_id,))
        art = await cursor.fetchone()
        if art:
            precio = art['costo_reposicion'] or art['costo'] or art['costo_promedio'] or 0
        else:
            precio = 0

        await cursor.execute("""
            INSERT INTO cmp_detalles_orden (enterprise_id, orden_id, articulo_id, cantidad_solicitada, precio_unitario, user_id_update)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (g.user['enterprise_id'], id, articulo_id, cantidad, precio, g.user['id']))
        
        # Recalcular total (simple sum)
        await cursor.execute("""
            UPDATE cmp_ordenes_compra SET total_estimado = (
                SELECT SUM(cantidad_solicitada * precio_unitario) FROM cmp_detalles_orden WHERE orden_id=%s
            ) WHERE id=%s
        """, (id, id))

    await flash("Ítem agregado.", "success")
    return redirect(url_for('compras.aprobar_po_detalle', id=id))

@compras_bp.route('/compras/aprobar_po/<int:id>', methods=['GET'])
@login_required
@permission_required('compras.aprobar_po')
async def aprobar_po_detalle(id):
    """Vista detallada para que el Gerente corrija, agregue items y apruebe la PO."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT o.*, p.nombre as proveedor_nombre, p.codigo as proveedor_codigo, p.email as proveedor_email, p.cuit as proveedor_cuit
            FROM cmp_ordenes_compra o
            JOIN erp_terceros p ON o.proveedor_id = p.id
            WHERE o.id = %s AND o.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        po = await cursor.fetchone()
        
        if not po:
            await flash("Orden no encontrada.", "danger")
            return redirect(url_for('compras.aprobaciones'))

        # Items de la PO
        await cursor.execute("""
            SELECT d.id, d.articulo_id, d.cantidad_solicitada as cantidad, d.precio_unitario, 
                   a.nombre as articulo_nombre, a.codigo as articulo_codigo
            FROM cmp_detalles_orden d
            JOIN stk_articulos a ON d.articulo_id = a.id
            WHERE d.orden_id = %s AND d.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        items = await cursor.fetchall()

        # Lista de Artículos para agregar (Buscador)
        await cursor.execute("SELECT id, nombre, codigo FROM stk_articulos WHERE enterprise_id = %s ORDER BY nombre", (g.user['enterprise_id'],))
        articulos = await cursor.fetchall()

        # PRESUPUESTO INFO (Fase 2)
        budget_status = None
        if po.get('centro_costo_id'):
            now = datetime.datetime.now()
            budget_status = await BudgetService.get_budget_status(g.user['enterprise_id'], po['centro_costo_id'], now.year, now.month)
            # Agregar nombre de centro de costos al objeto po para mostrarlo en cabecera
            await cursor.execute("SELECT name, code FROM sys_cost_centers WHERE id = %s", (po['centro_costo_id'],))
            cc_data = await cursor.fetchone()
            if cc_data:
                po['cost_center_name'] = cc_data['name']
                po['cost_center_code'] = cc_data['code']

        # WORKFLOW INFO (Auditoria)
        wf_state = await WorkflowService.get_approval_state(g.user['enterprise_id'], 'CMP_PO', id)
        wf_history = await WorkflowService.get_workflow_history(g.user['enterprise_id'], 'CMP_PO', id)

    return await render_template('compras/aprobar_po_detalle.html', 
                          po=po, items=items, articulos=articulos, 
                          wf_state=wf_state, wf_history=wf_history,
                          budget_status=budget_status)

@compras_bp.route('/compras/post_aprobacion_po/<int:id>', methods=['POST'])
@login_required
@permission_required('compras.aprobar_po')
async def post_aprobacion_po(id):
    """Procesa la aprobación/corrección de la PO por el Gerente."""
    action = (await request.form).get('action') # 'approve' or 'reject'
    observaciones = (await request.form).get('observaciones', '')
    
    async with get_db_cursor(dictionary=True) as cursor:
        # --- SEGREGACIÓN DE FUNCIONES (SoD) ---
        await cursor.execute("SELECT user_id, centro_costo_id FROM cmp_ordenes_compra WHERE id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
        po_meta = await cursor.fetchone()
        if po_meta and po_meta['user_id'] == g.user['id']:
            await flash("Seguridad CISA: No puede aprobar una orden creada por usted mismo (Segregación de Funciones).", "danger")
            return redirect(url_for('compras.aprobar_po_detalle', id=id))

        if action == 'approve':
            # 1. Calcular total y actualizar ítems
            total_estimado = 0
            for key, value in (await request.form).items():
                if key.startswith('cant_'):
                    item_id = key.split('_')[1]
                    try:
                        cant = float(value)
                        u_price = float((await request.form).get(f'price_{item_id}', 0))
                        total_estimado += cant * u_price
                        
                        await cursor.execute("""
                            UPDATE cmp_detalles_orden 
                            SET cantidad_solicitada = %s, precio_unitario = %s, user_id_update = %s 
                            WHERE id = %s AND orden_id = %s AND enterprise_id = %s
                        """, (cant, u_price, g.user['id'], item_id, id, g.user['enterprise_id']))
                    except: pass

            # --- CONTROL DE PRESUPUESTO (FASE 2) ---
            if po_meta and po_meta['centro_costo_id']:
                budget_check = await BudgetService.check_funds_for_po(g.user['enterprise_id'], po_meta['centro_costo_id'], total_estimado)
                if not budget_check['success']:
                    await flash(f"Bloqueo de Presupuesto: {budget_check['message']}", "danger")
                    return redirect(url_for('compras.aprobar_po_detalle', id=id))

            # 2. Asegurar que el workflow esté iniciado
            await WorkflowService.start_workflow(g.user['enterprise_id'], 'CMP_PO', id, total_estimado)
            
            # 3. Procesar paso del workflow
            res = await WorkflowService.approve_step(
                g.user['enterprise_id'], 
                'CMP_PO', 
                id, 
                g.user['id'], 
                g.user['role_id'],
                comment=observaciones
            )
            
            if res['success']:
                if res['final']:
                    # Aprobación definitiva completada (todos los niveles pasados)
                    
                    # Comprometer Fondos Reales
                    if po_meta and po_meta['centro_costo_id']:
                        await BudgetService.commit_funds(g.user['enterprise_id'], 'PO', id, po_meta['centro_costo_id'], total_estimado)

                    await cursor.execute("""
                        UPDATE cmp_ordenes_compra 
                        SET estado = 'ENVIADA_TESORERIA', 
                            total_estimado = %s,
                            aprobador_compras_id = %s,
                            fecha_aprobacion_compras = NOW(),
                            observaciones_rechazo = NULL,
                            user_id_update = %s
                        WHERE id = %s AND enterprise_id = %s
                    """, (total_estimado, g.user['id'], g.user['id'], id, g.user['enterprise_id']))
                    await flash(f"PO #{id}: {res['message']} Fondos comprometidos exitosamente.", "success")
                else:
                    # Falta algún nivel o firma
                    await cursor.execute("""
                        UPDATE cmp_ordenes_compra 
                        SET total_estimado = %s, user_id_update = %s
                        WHERE id = %s AND enterprise_id = %s
                    """, (total_estimado, g.user['id'], id, g.user['enterprise_id']))
                    await flash(f"PO #{id}: {res['message']}", "info")
            else:
                await flash(res['message'], "warning")
            
        elif action == 'reject':
            await cursor.execute("""
                UPDATE cmp_ordenes_compra 
                SET estado = 'RECHAZADA_COMPRAS',
                    observaciones_rechazo = %s
                WHERE id = %s AND enterprise_id = %s
            """, (observaciones, id, g.user['enterprise_id']))
            await flash(f"PO #{id} rechazada.", "warning")

    return redirect(url_for('compras.aprobaciones'))

@compras_bp.route('/compras/api/buscar-proveedores')
@login_required
async def api_buscar_proveedores():
    """Búsqueda dinámica de proveedores para selectores inteligentes.
    Parámetro: q (texto a buscar por código, nombre, CUIT, email o localidad).
    Devuelve JSON con id, codigo, nombre, cuit, localidad, tipo_responsable, condicion_iibb, iibb_condicion.
    """
    q = request.args.get('q', '').strip()
    enterprise_id = g.user['enterprise_id']

    async with get_db_cursor(dictionary=True) as cursor:
        if q and len(q) >= 2:
            like = f'%{q}%'
            await cursor.execute("""
                SELECT t.id, t.codigo, t.nombre, t.cuit,
                       COALESCE(d.localidad, '') as localidad,
                       t.tipo_responsable, t.condicion_iibb, t.iibb_condicion,
                       t.email
                FROM erp_terceros t
                LEFT JOIN erp_direcciones d ON t.id = d.tercero_id AND d.es_fiscal = 1
                WHERE (t.enterprise_id = %s OR t.enterprise_id = 0)
                  AND t.es_proveedor = 1
                  AND t.activo = 1
                  AND (
                      t.nombre    LIKE %s OR
                      t.cuit      LIKE %s OR
                      t.codigo    LIKE %s OR
                      t.email     LIKE %s OR
                      d.localidad LIKE %s
                  )
                GROUP BY t.id
                ORDER BY t.nombre
                LIMIT 30
            """, (enterprise_id, like, like, like, like, like))
        else:
            await cursor.execute("""
                SELECT t.id, t.codigo, t.nombre, t.cuit,
                       COALESCE(d.localidad, '') as localidad,
                       t.tipo_responsable, t.condicion_iibb, t.iibb_condicion,
                       t.email
                FROM erp_terceros t
                LEFT JOIN erp_direcciones d ON t.id = d.tercero_id AND d.es_fiscal = 1
                WHERE (t.enterprise_id = %s OR t.enterprise_id = 0)
                  AND t.es_proveedor = 1
                  AND t.activo = 1
                GROUP BY t.id
                ORDER BY t.nombre
                LIMIT 30
            """, (enterprise_id,))
        rows = await cursor.fetchall()

    return jsonify(rows)


@compras_bp.route('/compras/api/ordenes-para-facturar', methods=['GET'])
@login_required
async def api_ordenes_para_facturar():
    """Lista POs en estado RECIBIDA o ENVIADA_TESORERIA para vincular con factura."""
    prov_id = request.args.get('proveedor_id')
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        query = """
            SELECT id, fecha_emision, total_estimado as total, estado
            FROM cmp_ordenes_compra
            WHERE enterprise_id = %s AND estado IN ('ENVIADA_TESORERIA', 'RECIBIDA_PARCIAL', 'RECIBIDA_TOTAL')
        """
        params = [ent_id]
        if prov_id:
            query += " AND proveedor_id = %s"
            params.append(prov_id)
            
        query += " ORDER BY fecha_emision DESC LIMIT 50"
        await cursor.execute(query, tuple(params))
        ordenes = await cursor.fetchall()
        
    return jsonify(ordenes)


@compras_bp.route('/compras/api/get_po_details/<int:id>', methods=['GET'])
@login_required
async def api_get_po_details(id):
    """Retorna los detalles de una PO para auto-completar la factura."""
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT o.*, p.nombre as proveedor_nombre, p.id as proveedor_id, p.tipo_responsable, p.condicion_iibb
            FROM cmp_ordenes_compra o
            JOIN erp_terceros p ON o.proveedor_id = p.id
            WHERE o.id = %s AND o.enterprise_id = %s
        """, (id, ent_id))
        po = await cursor.fetchone()
        
        if not po:
            return jsonify({'success': False, 'message': 'Orden no encontrada'}), 404
            
        await cursor.execute("""
            SELECT d.*, a.nombre as articulo_nombre, a.codigo as articulo_codigo
            FROM cmp_detalles_orden d
            JOIN stk_articulos a ON d.articulo_id = a.id
            WHERE d.orden_id = %s AND d.enterprise_id = %s
        """, (id, ent_id))
        items = await cursor.fetchall()
        
        return jsonify({'success': True, 'po': po, 'items': items})


@compras_bp.route('/compras/facturar', methods=['GET'])
@login_required
async def facturar():
    async with get_db_cursor(dictionary=True) as cursor:
        # Proveedores
        await cursor.execute("SELECT id, codigo, nombre, cuit, tipo_responsable, condicion_iibb, iibb_condicion FROM erp_terceros WHERE enterprise_id = %s AND es_proveedor = 1 AND activo = 1", (g.user['enterprise_id'],))
        proveedores = await cursor.fetchall()
        
        # Tipos de Comprobante
        await cursor.execute("SELECT codigo, descripcion FROM sys_tipos_comprobante")
        tipos = await cursor.fetchall()
        
        # Depósitos
        await cursor.execute("SELECT id, nombre FROM stk_depositos WHERE enterprise_id = %s AND activo = 1", (g.user['enterprise_id'],))
        depositos = await cursor.fetchall()

        # Jurisdicciones
        await cursor.execute("SELECT codigo, nombre FROM sys_jurisdicciones ORDER BY codigo")
        jurisdicciones = await cursor.fetchall()

        # Artículos (para costeo directo desde factura local)
        await cursor.execute("SELECT id, codigo, nombre FROM stk_articulos WHERE enterprise_id = %s AND activo = 1", (g.user['enterprise_id'],))
        articulos = await cursor.fetchall()

    import datetime
    return await render_template('compras/facturar.html', 
                          proveedores=proveedores, 
                          tipos_comprobante=tipos, 
                          depositos=depositos, 
                          jurisdicciones=jurisdicciones,
                          articulos=articulos,
                          now=datetime.date.today().strftime('%Y-%m-%d'))

@compras_bp.route('/compras/api/guardar-comprobante', methods=['POST'])
@login_required
async def guardar_comprobante_api():
    from quart import jsonify
    data = (await request.json)
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            # Calcular totales globales
            neto_total = data.get('neto_21', 0) + data.get('neto_10_5', 0) + data.get('neto_27', 0)
            iva_total = data.get('iva_21', 0) + data.get('iva_10_5', 0) + data.get('iva_27', 0)
            otros_total = data.get('exento', 0) + data.get('no_gravado', 0) + data.get('perc_iva', 0) + data.get('perc_arba', 0) + data.get('perc_agip', 0) + data.get('otros_imp', 0)
            total = neto_total + iva_total + otros_total
            
            import datetime
            fecha_vencimiento = None
            # Validar si viene una condición de pago y calcular fecha vencimiento
            cond_pago = data.get('condicion_pago_id')
            if cond_pago:
                await cursor.execute("SELECT dias_vencimiento FROM fin_condiciones_pago WHERE id = %s", (cond_pago,))
                cond_row = await cursor.fetchone()
                if cond_row and cond_row['dias_vencimiento'] is not None:
                    fv = datetime.date.fromisoformat(data['fecha']) + datetime.timedelta(days=int(cond_row['dias_vencimiento']))
                    fecha_vencimiento = fv.isoformat()

            # Cabecera
            # Obtener CUIT de la empresa (Receptor) y del proveedor (Emisor)
            await cursor.execute("SELECT cuit FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
            ent_row = await cursor.fetchone()
            ent_cuit = ent_row['cuit'] if ent_row else ''

            await cursor.execute("SELECT cuit FROM erp_terceros WHERE id = %s", (data['proveedor_id'],))
            prov_row = await cursor.fetchone()
            prov_cuit = prov_row['cuit'] if prov_row else ''

            await cursor.execute("""
                INSERT INTO erp_comprobantes (
                    enterprise_id, modulo, tipo_operacion, emisor_cuit, receptor_cuit,
                    tercero_id, orden_compra_id, tipo_comprobante, punto_venta, numero, fecha_emision, fecha_vencimiento, 
                    importe_neto, importe_iva, importe_total, 
                    importe_exento, importe_no_gravado, importe_percepcion_iva, 
                    importe_percepcion_iibb_arba, importe_percepcion_iibb_agip, 
                    jurisdiccion_id,
                    importe_impuestos_internos,
                    estado_pago,
                    condicion_pago_id
                )
                VALUES (%s, 'COMPRAS', 'COMPRA', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDIENTE', %s)
            """, (
                g.user['enterprise_id'], prov_cuit, ent_cuit,
                data['proveedor_id'], data.get('orden_compra_id'), data['tipo_comprobante'], data['punto_venta'] or 1, 
                data['numero'], data['fecha'], fecha_vencimiento, neto_total, iva_total, total,
                data.get('exento', 0), data.get('no_gravado', 0), data.get('perc_iva', 0), 
                data.get('perc_arba', 0), data.get('perc_agip', 0), 
                data.get('jurisdiccion_id') or None,
                data.get('otros_imp', 0),
                cond_pago
            ))
            
            comp_id = cursor.lastrowid
            
            # Detalle de Items (Si vienen detallados) o por Alícuota
            items_enviados = data.get('items', [])
            if items_enviados:
                for it in items_enviados:
                    await cursor.execute("""
                        INSERT INTO erp_comprobantes_detalle (enterprise_id, comprobante_id, articulo_id, detalle_po_id, cantidad, precio_unitario, alicuota_iva, subtotal_neto, subtotal_total)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (g.user['enterprise_id'], comp_id, it['articulo_id'], it.get('detalle_po_id'), it['cantidad'], it['precio_unitario'], it['alicuota_iva'], 
                          float(it['cantidad']) * float(it['precio_unitario']), float(it['cantidad']) * float(it['precio_unitario']) * (1 + float(it['alicuota_iva'])/100)))
            
            # --- VALIDACION 3-WAY MATCH ---
            po_id = data.get('orden_compra_id')
            match_msg = ""
            if po_id:
                from services.receiving_service import ReceivingService
                match_res = await ReceivingService.match_invoice_vs_receipt(g.user['enterprise_id'], po_id, items_enviados)
                if not match_res['success']:
                    match_msg = " | ⚠️ ATENCIÓN: Discrepancias en 3-Way Match: " + "; ".join(match_res['discrepancies'])
                else:
                    # Si no hay discrepancias, marcar PO como FACTURADA
                    await cursor.execute("UPDATE cmp_ordenes_compra SET estado = 'FACTURADA' WHERE id = %s", (po_id,))
            else:
                # Fallback: Detalle por Alícuota (Solo si hay importe en ese neto)
                tasas = [
                    ('21', data['neto_21'], data['iva_21']),
                    ('10.5', data['neto_10_5'], data['iva_10_5']),
                    ('27', data['neto_27'], data['iva_27'])
                ]
                for tasa, neto, iva in tasas:
                    if neto > 0 or iva > 0:
                        await cursor.execute("""
                            INSERT INTO erp_comprobantes_detalle (enterprise_id, comprobante_id, descripcion, cantidad, precio_unitario, alicuota_iva, subtotal_neto, importe_iva, subtotal_total)
                            VALUES (%s, %s, %s, 1, %s, %s, %s, %s, %s)
                        """, (g.user['enterprise_id'], comp_id, f"Compra Mercadería / Serv. - Tasa {tasa}%", neto, tasa, neto, iva, neto + iva))
            
            # --- GUARDAR DETALLES DE IMPUESTOS DINÁMICOS EN COMPRAS ---
            # Guardamos los impuestos extra como ARBA y AGIP que tipearon (mismo patrón trazabilidad)
            impuestos_a_insertar = []
            if data.get('perc_arba', 0) > 0: impuestos_a_insertar.append(('PERCEPCION', 'BUENOS AIRES', data['perc_arba']))
            if data.get('perc_agip', 0) > 0: impuestos_a_insertar.append(('PERCEPCION', 'CABA', data['perc_agip']))
            if data.get('perc_iva', 0) > 0: impuestos_a_insertar.append(('PERCEPCION IVA', 'NACIONAL', data['perc_iva']))
            
            for tipo, juris, importe in impuestos_a_insertar:
                await cursor.execute("""
                    INSERT INTO erp_comprobantes_impuestos (enterprise_id, comprobante_id, jurisdiccion, importe, user_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (g.user['enterprise_id'], comp_id, juris, importe, g.user['id']))
            
            # --- GENERAR ASIENTO CONTABLE O PROPUESTAS DE PRECIOS ---
            # Ahora _generar_asiento_contable_compra decide si genera asiento o deferencia a Pricing
            asiento_id = await _generar_asiento_contable_compra(cursor, comp_id, g.user['enterprise_id'], g.user['id'], items=items_enviados)
            if asiento_id:
                await cursor.execute("UPDATE erp_comprobantes SET asiento_id = %s WHERE id = %s", (asiento_id, comp_id))
            
        return jsonify({'success': True, 'message': 'Comprobante guardado' + match_msg})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@compras_bp.route('/compras/pagar', methods=['GET'])
@login_required
async def pagar():
    # Proveedores desde servicio centralizado
    proveedores = await TerceroService.get_proveedores_for_selector(g.user['enterprise_id'])

    async with get_db_cursor(dictionary=True) as cursor:
        # Medios de Pago
        await cursor.execute("""
            SELECT id, nombre, tipo 
            FROM fin_medios_pago 
            WHERE enterprise_id = %s AND activo = 1 
            AND tipo NOT IN ('RETENCION', 'PERCEPCION')
            ORDER BY nombre
        """, (g.user['enterprise_id'],))
        medios_pago = await cursor.fetchall()

    import datetime
    return await render_template('compras/pagar.html', 
                          proveedores=proveedores, 
                          medios_pago=medios_pago,
                          now=datetime.date.today().isoformat())

@compras_bp.route('/compras/api/facturas-pendientes-proveedor/<int:proveedor_id>')
@login_required
async def api_facturas_pendientes_proveedor(proveedor_id):
    import datetime
    async with get_db_cursor(dictionary=True) as cursor:
        # Obtener facturas con saldo pendiente
        # Calculamos saldo como Total - Pagado
        await cursor.execute("""
            SELECT c.*, tc.descripcion as tipo_nombre,
                   (c.importe_total - COALESCE(
                       (SELECT SUM(importe_pagado) FROM fin_ordenes_pago_comprobantes WHERE comprobante_id = c.id AND enterprise_id = c.enterprise_id), 0
                   )) as saldo
            FROM erp_comprobantes c
            JOIN sys_tipos_comprobante tc ON c.tipo_comprobante = tc.codigo
            WHERE c.tercero_id = %s 
              AND c.enterprise_id = %s
              AND c.modulo = 'COMPRAS'
            HAVING (c.importe_total - COALESCE(
                       (SELECT SUM(importe_pagado) FROM fin_ordenes_pago_comprobantes WHERE comprobante_id = c.id AND enterprise_id = c.enterprise_id), 0
                   )) > 0.05
            ORDER BY c.fecha_emision
        """, (proveedor_id, g.user['enterprise_id']))
        facturas = await cursor.fetchall()
        
    return jsonify(facturas)

@compras_bp.route('/compras/api/procesar-orden-pago', methods=['POST'])
@login_required
@atomic_transaction('compras', severity=9, impact_category='Financial')
async def api_procesar_orden_pago():
    from quart import jsonify
    data = (await request.json)
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            # --- CIRUGÍA DE SEGURIDAD: CONTROL DE TRAIDORES (APOC) ---
            ent_id = g.user['enterprise_id']
            proveedor_id = data.get('proveedor_id')
            
            await cursor.execute("SELECT cuit, nombre FROM erp_terceros WHERE id = %s AND enterprise_id = %s", (proveedor_id, ent_id))
            prov_data = await cursor.fetchone()
            if prov_data:
                apoc = await AfipService.consultar_base_apoc(ent_id, prov_data['cuit'])
                if apoc.get('es_apocrifo'):
                    await AfipService.registrar_bitacora(ent_id, "INTENTO_PAGO_TRAIDOR", "CRITICAL", f"SE BLOQUEÓ PAGO a {prov_data['nombre']}. Sujeto marcado como APÓCRIFO.")
                    return jsonify({
                        'success': False, 
                        'error': f"🛑 BLOQUEO DE SEGURIDAD: El proveedor {prov_data['nombre']} ha sido detectado como COMPROMETIDO (APÓCRIFO) en la Matrix AFIP. No se pueden procesar pagos."
                    }), 403

            # 1. Obtener Próximo Número
            from services.numeration_service import NumerationService
            nro_op = await NumerationService.get_next_number(ent_id, 'ORDEN_PAGO', 'OP', 1)
            
            total_op = sum([float(f['importe']) for f in data['facturas']])
            total_ret = sum([float(r['importe']) for r in data['retenciones']])
            
            await cursor.execute("""
                INSERT INTO fin_ordenes_pago (enterprise_id, numero, fecha, tercero_id, importe_total, importe_retenciones, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (ent_id, nro_op, data['fecha'], data['proveedor_id'], total_op, total_ret, g.user['id']))
            op_id = cursor.lastrowid
            
            # Actualizar numeración
            await NumerationService.update_last_number(ent_id, 'ORDEN_PAGO', 'OP', 1, nro_op)
            
            for f in data['facturas']:
                await cursor.execute("""
                    INSERT INTO fin_ordenes_pago_comprobantes (enterprise_id, orden_pago_id, comprobante_id, importe_pagado, user_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (ent_id, op_id, f['id'], f['importe'], g.user['id']))
                
                await cursor.execute("""
                    UPDATE erp_comprobantes 
                    SET estado_pago = IF(
                        (importe_total - COALESCE((SELECT SUM(importe_pagado) FROM fin_ordenes_pago_comprobantes WHERE comprobante_id = %s AND enterprise_id = %s), 0)) < 0.05,
                        'PAGADO', 'PARCIAL'
                    )
                    WHERE id = %s AND enterprise_id = %s
                """, (f['id'], f['id'], ent_id, f['id'], ent_id))

            for m in data['medios']:
                await cursor.execute("SELECT cuenta_contable_id FROM fin_medios_pago WHERE id = %s", (m['id'],))
                mp_row = await cursor.fetchone()
                cuenta_snapshot_id = mp_row['cuenta_contable_id'] if mp_row else None

                await cursor.execute("""
                    INSERT INTO fin_ordenes_pago_medios (enterprise_id, orden_pago_id, medio_pago_id, cuenta_contable_snapshot_id, importe, es_echeck, debin_id, banco_id, nro_cheque, fecha_pago, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (ent_id, op_id, m['id'], cuenta_snapshot_id, m['monto'], m.get('es_echeck', 0), m.get('debin_id'), m.get('banco_id'), m.get('nro_cheque'), m.get('fecha_pago'), g.user['id']))

            for r in data['retenciones']:
                if float(r['importe']) > 0:
                    await cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 as proximo FROM fin_retenciones_emitidas")
                    next_id = await cursor.fetchone()['proximo']
                    nro_cert = f"CRT-{r['tipo']}-{str(next_id).zfill(6)}"
                    
                    await cursor.execute("""
                        INSERT INTO fin_retenciones_emitidas (enterprise_id, comprobante_pago_id, tipo_retencion, numero_certificado, fecha, tercero_id, importe_retencion, user_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (ent_id, op_id, r['tipo'], nro_cert, data['fecha'], data['proveedor_id'], r['importe'], g.user['id']))

            # 3.b. Generar Asiento de OP
            asiento_op_id = await _generar_asiento_orden_pago(cursor, op_id, ent_id, g.user['id'])
            if asiento_op_id:
                pass 

        # 4. Enviar Mail con Certificados (si hay retenciones)
        if data['retenciones']:
            try:
                async with get_db_cursor(dictionary=True) as cursor:
                    await cursor.execute("SELECT nombre, email FROM erp_terceros WHERE id = %s AND enterprise_id = %s", (data['proveedor_id'], ent_id))
                    prov = await cursor.fetchone()
                    
                    if prov and prov['email']:
                        await cursor.execute("""
                            SELECT c.fecha_emision, c.punto_venta, c.numero, tc.descripcion as tipo_nombre, fopc.importe_pagado
                            FROM fin_ordenes_pago_comprobantes fopc
                            JOIN erp_comprobantes c ON fopc.comprobante_id = c.id
                            JOIN sys_tipos_comprobante tc ON c.tipo_comprobante = tc.codigo
                            WHERE fopc.orden_pago_id = %s
                        """, (op_id,))
                        facturas_det = await cursor.fetchall()

                        for r in data['retenciones']:
                            if float(r['importe']) > 0:
                                await cursor.execute("SELECT * FROM fin_retenciones_emitidas WHERE comprobante_pago_id = %s AND tipo_retencion = %s AND enterprise_id = %s", (op_id, r['tipo'], ent_id))
                                cert_db = await cursor.fetchone()
                                
                                await cursor.execute("""
                                    SELECT r.*, t.nombre as sujeto_nombre, t.cuit as sujeto_cuit, t.tipo_responsable as sujeto_iva,
                                    d.calle, d.numero, d.localidad, d.provincia
                                    FROM fin_retenciones_emitidas r
                                    JOIN erp_terceros t ON r.tercero_id = t.id
                                    LEFT JOIN erp_direcciones d ON t.id = d.tercero_id AND d.es_fiscal = 1
                                    WHERE r.id = %s AND r.enterprise_id = %s
                                """, (cert_db['id'], ent_id))
                                ret_full = await cursor.fetchone()
                                
                                await cursor.execute("SELECT * FROM sys_enterprises WHERE id = %s", (ent_id,))
                                empresa = await cursor.fetchone()

                                html_pdf = await render_template('compras/certificado_retencion.html', ret=ret_full, empresa=empresa)
                                
                                pdf_out = io.BytesIO()
                                pisa.CreatePDF(io.StringIO(html_pdf), dest=pdf_out)
                                pdf_content = pdf_out.getvalue()
                                
                                subject, html_body = await enviar_notificacion_retencion(
                                    prov['email'], prov['nombre'], cert_db['numero_certificado'], 
                                    r['tipo'], r['importe'], facturas_det, ent_id
                                )
                                
                                pdf_filename = f"Certificado_{r['tipo']}_{cert_db['numero_certificado']}.pdf"
                                await _enviar_email(prov['email'], subject, html_body, [(pdf_filename, pdf_content)], enterprise_id=ent_id)
            except Exception as mail_err:
                print(f"Error enviando notificaciones: {mail_err}")

        return jsonify({'success': True, 'op_id': op_id})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500

@compras_bp.route('/compras/ordenes-pago')
@login_required
async def ordenes_pago():
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT o.*, t.nombre as proveedor_nombre
            FROM fin_ordenes_pago o
            JOIN erp_terceros t ON o.tercero_id = t.id
            WHERE o.enterprise_id = %s
            ORDER BY o.fecha DESC, o.numero DESC
        """, (g.user['enterprise_id'],))
        ops = await cursor.fetchall()
        
        await cursor.execute("""
            SELECT r.*, t.nombre as sujeto_nombre, op.numero as op_numero
            FROM fin_retenciones_emitidas r
            JOIN erp_terceros t ON r.tercero_id = t.id
            LEFT JOIN fin_ordenes_pago op ON r.comprobante_pago_id = op.id
            WHERE r.enterprise_id = %s
            ORDER BY r.fecha DESC
        """, (g.user['enterprise_id'],))
        retenciones = await cursor.fetchall()

    return await render_template('compras/ordenes_pago_lista.html', ops=ops, retenciones=retenciones)

@compras_bp.route('/compras/ver-retencion/<int:id>')
@login_required
async def ver_retencion(id):
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT r.*, t.nombre as sujeto_nombre, t.cuit as sujeto_cuit, t.tipo_responsable as sujeto_iva,
            d.calle, d.numero, d.localidad, d.provincia
            FROM fin_retenciones_emitidas r
            JOIN erp_terceros t ON r.tercero_id = t.id
            LEFT JOIN erp_direcciones d ON t.id = d.tercero_id AND d.es_fiscal = 1
            WHERE r.id = %s AND r.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        ret = await cursor.fetchone()
        
        await cursor.execute("SELECT * FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
        empresa = await cursor.fetchone()
        
    return await render_template('compras/certificado_retencion.html', ret=ret, empresa=empresa)

# /compras/ordenes-compra is now an alias redirecting to the canonical ordenes endpoint
@compras_bp.route('/compras/ordenes-compra', endpoint='ordenes_compra_alias')
@login_required
def ordenes_compra_alias():
    return redirect(url_for('compras.ordenes'))

@compras_bp.route('/compras/registro-afip', endpoint='registro_afip')
@login_required
async def registro_afip():
    await flash("Módulo en construcción: Registro AFIP de Compras", "info")
    return redirect(url_for('compras.dashboard'))

@compras_bp.route('/compras/cotizacion/<int:id>/reenviar', methods=['POST'])
@login_required
async def reenviar_cotizacion(id):
    from services.quotation_mailer import QuotationMailer
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("""
                SELECT c.*, p.nombre as razon_social, p.email as proveedor_email, e.nombre as empresa_nombre
                FROM cmp_cotizaciones c
                JOIN erp_terceros p ON c.proveedor_id = p.id
                JOIN sys_enterprises e ON c.enterprise_id = e.id
                WHERE c.id = %s AND c.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            cot = await cursor.fetchone()
            
            if not cot:
                await flash("Cotización no encontrada.", "danger")
                return redirect(url_for('compras.cotizaciones'))

            if not cot['proveedor_email']:
                await flash("El proveedor no tiene un correo electrónico configurado.", "warning")
                return redirect(url_for('compras.cotizacion_detalle', id=id))

            # Obtener items
            await cursor.execute("""
                SELECT i.articulo_id, a.codigo as codigo_interno, a.nombre as nombre_articulo, i.cantidad
                FROM cmp_items_cotizacion i
                JOIN stk_articulos a ON i.articulo_id = a.id
                WHERE i.cotizacion_id = %s AND i.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            items = await cursor.fetchall()

        mailer = QuotationMailer(g.user['enterprise_id'])
        excel_path = mailer.generate_excel_attachment(id, cot['razon_social'], items, cot['security_hash'])
        html_body = mailer.generate_html_body(cot['empresa_nombre'], id, cot['security_hash'])
        
        success, error = await mailer.send_email_real(
            to_email=cot['proveedor_email'],
            subject=f"RE-ENVIO: Solicitud Cotización #{id} - REF: {cot['security_hash'][:10]}",
            body=html_body,
            attachment_path=excel_path
        )
        
        if success:
            await flash(f"✅ Correo re-enviado con éxito a {cot['proveedor_email']}.", "success")
        else:
            await flash(f"⚠️ Error al re-enviar correo: '{error}'. Verifique la configuración de correo de la empresa o contacte al administrador.", "warning")
            
    except Exception as e:
        await flash(f"Error inesperado: {str(e)}", "danger")
        
    return redirect(url_for('compras.cotizacion_detalle', id=id))

@compras_bp.route('/compras/po/<int:id>/reenviar', methods=['POST'])
@login_required
async def reenviar_po(id):
    from services.purchase_order_mailer import PurchaseOrderMailer
    from services.workflow_service import WorkflowService
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("""
                SELECT o.*, p.nombre as proveedor_nombre, p.email as proveedor_email, e.nombre as empresa_nombre
                FROM cmp_ordenes_compra o
                JOIN erp_terceros p ON o.proveedor_id = p.id
                JOIN sys_enterprises e ON o.enterprise_id = e.id
                WHERE o.id = %s AND o.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            po = await cursor.fetchone()
            
            if not po:
                await flash("Orden de compra no encontrada.", "danger")
                return redirect(url_for('compras.aprobaciones'))

            if not po['proveedor_email']:
                await flash("El proveedor no tiene un correo electrónico configurado.", "warning")
                return redirect(url_for('compras.aprobar_po_detalle', id=id))

            # Obtener items
            await cursor.execute("""
                SELECT i.*, a.codigo as articulo_codigo, a.nombre as articulo_nombre
                FROM cmp_detalles_orden i
                JOIN stk_articulos a ON i.articulo_id = a.id
                WHERE i.orden_id = %s AND i.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            items = await cursor.fetchall()

        mailer = PurchaseOrderMailer(g.user['enterprise_id'])
        excel_path = await mailer.generate_excel_po(id, po['proveedor_nombre'], items, po['security_hash'])
        
        success, error = await mailer.send_po_email(
            to_email=po['proveedor_email'],
            po_id=id,
            po_hash=po['security_hash'],
            excel_path=excel_path,
            empresa_nombre=po['empresa_nombre']
        )
        
        if success:
            await flash(f"✅ Correo de Orden de Compra re-enviado con éxito a {po['proveedor_email']}.", "success")
        else:
            await flash(f"⚠️ Error al re-enviar correo: '{error}'. Verifique la configuración de correo de la empresa o contacte al administrador.", "warning")
            
    except Exception as e:
        await flash(f"Error inesperado: {str(e)}", "danger")
        
    return redirect(url_for('compras.aprobar_po_detalle', id=id))

@compras_bp.route('/compras/api/verificar-cae', methods=['POST'])
@login_required
async def api_verificar_cae():
    data = (await request.json)
    try:
        from services.afip_service import AfipService
        # En la vida real, AFIP no permite 'Consultar' facturas de terceros directamente por webservice WSFE 
        # (solo permite consultar las propias enviadas).
        # Lo que se hace es una 'Constatación de Comprobantes'. 
        # Por ahora, simularemos la respuesta o usaremos un Mock si no hay certificados.
        
        # Simulación de validación
        cae = data.get('cae')
        cuit_emisor = data.get('cuit_emisor')
        
        if not cae or not cuit_emisor:
            return jsonify({'success': False, 'message': 'Faltan datos (CAE o CUIT Emisor)'})

        # Simulamos que si el CAE termina en '0', es apócrifa/inválida
        if str(cae).endswith('0'):
             return jsonify({
                'success': False, 
                'error_afip': 'El comprobante no consta en los registros de AFIP o los datos no coinciden.',
                'status': 'RECHAZADO'
            })
            
        return jsonify({
            'success': True,
            'message': 'Comprobante válido en los registros de AFIP.',
            'data': {
                'cae': cae,
                'fecha_proceso': datetime.datetime.now().strftime('%d/%m/%Y %H:%M'),
                'observaciones': 'La consulta ha sido procesada exitosamente.'
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# --- ADMINISTRACIÓN DE WORKFLOWS ("My Money, My Decision") ---

@compras_bp.route('/compras/admin/workflows')
@login_required
@permission_required('admin_compras')
async def admin_workflows():
    """Panel de configuración de reglas de aprobación para el dueño/administrador."""
    async with get_db_cursor(dictionary=True) as cursor:
        # Traer reglas de la empresa o globales (0)
        await cursor.execute("""
            SELECT r.*, 
                   (SELECT COUNT(*) FROM sys_workflow_steps WHERE rule_id = r.id) as step_count
            FROM sys_workflow_rules r
            WHERE r.enterprise_id IN (%s, 0) AND r.module = 'COMPRAS'
            ORDER BY r.enterprise_id DESC, r.priority ASC
        """, (g.user['enterprise_id'],))
        rules = await cursor.fetchall()
        
        # Traer roles disponibles para el selector de pasos
        await cursor.execute("SELECT id, name FROM sys_roles WHERE enterprise_id IN (%s, 0)", (g.user['enterprise_id'],))
        roles = await cursor.fetchall()

    return await render_template('compras/admin_workflows.html', rules=rules, roles=roles)

@compras_bp.route('/compras/admin/workflows/rule/<int:rule_id>', methods=['GET'])
@login_required
@permission_required('admin_compras')
async def get_workflow_rule_details(rule_id):
    """Retorna los pasos y detalles de una regla para edición AJAX."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM sys_workflow_rules WHERE id = %s", (rule_id,))
        rule = await cursor.fetchone()
        
        await cursor.execute("SELECT * FROM sys_workflow_steps WHERE rule_id = %s ORDER BY step_order", (rule_id,))
        steps = await cursor.fetchall()
        
    return jsonify({'rule': rule, 'steps': steps})

@compras_bp.route('/compras/admin/workflows/save', methods=['POST'])
@login_required
@permission_required('admin_compras')
async def save_workflow_config():
    """Guarda la configuración de una regla (Montos y Pasos)."""
    data = (await request.json)
    rule_id = data.get('rule_id')
    new_amount = data.get('condition_value')
    # steps es una lista de objetos: {step_order, role_id, description}
    steps_list = data.get('steps', []) 

    try:
        async with get_db_cursor() as cursor:
            # 1. Actualizar monto de la regla
            await cursor.execute("""
                UPDATE sys_workflow_rules 
                SET condition_value = %s, user_id_update = %s 
                WHERE id = %s AND enterprise_id IN (%s, 0)
            """, (new_amount, g.user['id'], rule_id, g.user['enterprise_id']))
            
            # 2. Re-generar pasos (Borrar y re-insertar para simplicidad)
            await cursor.execute("DELETE FROM sys_workflow_steps WHERE rule_id = %s", (rule_id,))
            
            for s in steps_list:
                await cursor.execute("""
                    INSERT INTO sys_workflow_steps (enterprise_id, rule_id, step_order, role_id, description, min_approvals)
                    VALUES (%s, %s, %s, %s, %s, 1)
                """, (g.user['enterprise_id'], rule_id, s['step_order'], s['role_id'], s['description']))
                
        return jsonify({'success': True, 'message': 'Configuración de Workflow guardada correctamente.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# ────────────────────────────────────────────────────────────────────────────
# ── FASE 3: RECEPCIÓN A CIEGAS Y 3-WAY MATCH ────────────────────────────────
# ────────────────────────────────────────────────────────────────────────────

@compras_bp.route('/compras/recepcion_ciega', methods=['GET'])
@login_required
@permission_required('view_compras')
async def recepcion_ciega_list():
    """Listado de Órdenes Pendientes de Recebir por Depósito."""
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT o.id, o.fecha_emision, p.nombre as proveedor_nombre, o.estado 
            FROM cmp_ordenes_compra o
            JOIN erp_terceros p ON o.proveedor_id = p.id
            WHERE o.enterprise_id = %s 
              AND o.estado IN ('ENVIADA_PROVEEDOR', 'EN_TRANSITO', 'RECIBIDA_PARCIAL', 'ENVIADA_TESORERIA')
            ORDER BY o.fecha_emision DESC
        """, (ent_id,))
        ordenes = await cursor.fetchall()

    return await render_template('compras/recepcion_ciega_list.html', ordenes=ordenes)

@compras_bp.route('/compras/recepcion_ciega/<int:po_id>', methods=['GET', 'POST'])
@login_required
@permission_required('view_compras')
@atomic_transaction('compras')
async def recepcion_ciega_procesar(po_id):
    """Procesa el ingreso físico ocultando cantidades pedidas."""
    ent_id = g.user['enterprise_id']
    
    if request.method == 'POST':
        # Procesar Formulario Front
        data = {
            'remito': (await request.form).get('numero_remito'),
            'observaciones': (await request.form).get('observaciones'),
            'items': {}
        }
        for key, val in (await request.form).items():
            if key.startswith('cant_') and val.strip() != '':
                try:
                    detalle_id = key.split('_')[1]
                    cant = float(val)
                    if cant > 0:
                        data['items'][detalle_id] = cant
                except:
                    pass
                    
        if not data['items']:
            await flash("Debe ingresar cantidad recibida en al menos un artículo.", "warning")
            return redirect(url_for('compras.recepcion_ciega_procesar', po_id=po_id))
            
        try:
            res = await ReceivingService.process_blind_receipt(ent_id, g.user['id'], po_id, data)
            if res['discrepancy']:
                await flash("Remito procesado. ⚠️ IMPORTANTE: Se han detectado discrepancias entre lo pedido y lo recibido. Tesorería ha sido notificada (3-Way Match Block).", "warning")
            else:
                await flash(f"Recepción procesada correctamente. <a href='#' onclick='window.open(\"/stock/dashboard?q=PO{po_id}\", \"_blank\"); return false;' class='btn btn-xs btn-outline-light ml-2'>Imprimir Etiquetas</a>", "success")
                
            return redirect(url_for('compras.recepcion_ciega_list'))
        except Exception as e:
            await flash(f"Error procesando recepción: {str(e)}", "danger")
            return redirect(url_for('compras.recepcion_ciega_procesar', po_id=po_id))

    # GET
    po_data = await ReceivingService.get_po_for_blind_receiving(ent_id, po_id)
    if not po_data:
        await flash("Orden no encontrada o no apta para recepción.", "warning")
        return redirect(url_for('compras.recepcion_ciega_list'))
        
    return await render_template('compras/recepcion_ciega.html', po=po_data)



