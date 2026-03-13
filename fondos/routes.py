from quart import Blueprint, render_template, request, g, redirect, url_for, flash, jsonify
from core.decorators import login_required, permission_required
from database import get_db_cursor, atomic_transaction
from services.bcra_service import BCRAService

fondos_bp = Blueprint('fondos', __name__, template_folder='templates')

@fondos_bp.route('/fondos/dashboard')
@login_required
async def dashboard():
    try:
        return await render_template('fondos/dashboard.html')
    except Exception as e:
        import traceback
        traceback.print_exc()
        await flash(f"Error al cargar el dashboard de fondos: {str(e)}", "danger")
        return redirect('/')

@fondos_bp.route('/fondos/cajas')
@login_required
async def cajas():
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM erp_cuentas_fondos WHERE enterprise_id = %s", (g.user['enterprise_id'],))
        cajas = await cursor.fetchall()
    return await render_template('fondos/cajas.html', cajas=cajas)

@fondos_bp.route('/fondos/medios-pago', methods=['GET', 'POST'])
@login_required
@atomic_transaction('fondos', severity=6, impact_category='Technical')
async def medios_pago():
    async with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            try:
                action = (await request.form).get('action')
                if action == 'create':
                    nombre = (await request.form)['nombre']
                    tipo = (await request.form)['tipo']
                    cuenta_contable_id = (await request.form).get('cuenta_contable_id') or None
                    recargo = (await request.form).get('recargo_pct', 0)
                    
                    await cursor.execute("""
                        INSERT INTO fin_medios_pago (enterprise_id, nombre, tipo, cuenta_contable_id, recargo_pct, user_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (g.user['enterprise_id'], nombre, tipo, cuenta_contable_id, recargo, g.user['id']))
                    await flash('Medio de pago creado exitosamente', 'success')
                    
                elif action == 'edit':
                    mid = (await request.form)['id']
                    nombre = (await request.form)['nombre']
                    tipo = (await request.form)['tipo']
                    cuenta_contable_id = (await request.form).get('cuenta_contable_id') or None
                    recargo = (await request.form).get('recargo_pct', 0)
                    activo = 1 if 'activo' in (await request.form) else 0
                    
                    await cursor.execute("""
                        UPDATE fin_medios_pago 
                        SET nombre=%s, tipo=%s, cuenta_contable_id=%s, recargo_pct=%s, activo=%s, user_id_update=%s 
                        WHERE id=%s AND enterprise_id=%s
                    """, (nombre, tipo, cuenta_contable_id, recargo, activo, g.user['id'], mid, g.user['enterprise_id']))
                    await flash('Medio de pago actualizado', 'success')
                
                elif action == 'delete':
                    mid = (await request.form)['id']
                    await cursor.execute("DELETE FROM fin_medios_pago WHERE id=%s AND enterprise_id=%s", (mid, g.user['enterprise_id']))
                    await flash('Medio de pago eliminado', 'warning')
                    
            except Exception as e:
                await flash(f'Error: {str(e)}', 'danger')
                
            return redirect(url_for('fondos.medios_pago'))
            
        # GET
        await cursor.execute("SELECT * FROM fin_medios_pago WHERE (enterprise_id = %s OR enterprise_id = 0) ORDER BY nombre", (g.user['enterprise_id'],))
        medios = await cursor.fetchall()
        for m in medios:
            if m.get('created_at'): m['created_at'] = str(m['created_at'])
            if m.get('updated_at'): m['updated_at'] = str(m['updated_at'])
        
        # Cuentas para asociar (Activos: Caja, Bancos, Creditos)
        await cursor.execute("SELECT id, codigo, nombre FROM cont_plan_cuentas WHERE tipo = 'ACTIVO' AND enterprise_id = %s ORDER BY codigo", (g.user['enterprise_id'],))
        cuentas = await cursor.fetchall()

    return await render_template('fondos/medios_pago.html', medios=medios, cuentas=cuentas)

# --- WORKFLOW DE TESORERIA ---

@fondos_bp.route('/fondos/aprobaciones')
@login_required
async def aprobaciones():
    """Listado de POs enviadas por Compras para aprobación de fondos."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT cmp_ordenes_compra.*, erp_terceros.nombre as proveedor_nombre 
            FROM cmp_ordenes_compra
            JOIN erp_terceros ON cmp_ordenes_compra.proveedor_id = erp_terceros.id
            WHERE cmp_ordenes_compra.estado = 'ENVIADA_TESORERIA' AND cmp_ordenes_compra.enterprise_id = %s
            ORDER BY cmp_ordenes_compra.fecha_aprobacion_compras ASC
        """, (g.user['enterprise_id'],))
        ordenes = await cursor.fetchall()
    return await render_template('fondos/aprobaciones_po.html', ordenes=ordenes)

@fondos_bp.route('/fondos/aprobar_po/<int:id>', methods=['GET'])
@login_required
async def aprobar_po_detalle(id):
    """Vista para que Tesorería apruebe el desembolso."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT cmp_ordenes_compra.*, erp_terceros.nombre as proveedor_nombre
            FROM cmp_ordenes_compra
            JOIN erp_terceros ON cmp_ordenes_compra.proveedor_id = erp_terceros.id
            WHERE cmp_ordenes_compra.id = %s AND cmp_ordenes_compra.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        po = await cursor.fetchone()
        
        if not po:
            await flash("Orden no encontrada.", "danger")
            return redirect(url_for('fondos.aprobaciones'))

        await cursor.execute("""
            SELECT cmp_detalles_orden.*, cmp_detalles_orden.cantidad_solicitada as cantidad, stk_articulos.nombre as articulo_nombre, stk_articulos.codigo as articulo_codigo
            FROM cmp_detalles_orden
            JOIN stk_articulos ON cmp_detalles_orden.articulo_id = stk_articulos.id
            WHERE cmp_detalles_orden.orden_id = %s AND cmp_detalles_orden.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        items = await cursor.fetchall()

    return await render_template('fondos/aprobar_po_detalle.html', po=po, items=items)

@fondos_bp.route('/fondos/post_aprobacion_po/<int:id>', methods=['POST'])
@login_required
@atomic_transaction('fondos', severity=9, impact_category='Financial')
async def post_aprobacion_po(id):
    """Procesa la aprobación de Tesorería."""
    action = (await request.form).get('action')
    fecha_pago = (await request.form).get('fecha_pago_estimada')
    observaciones = (await request.form).get('observaciones', '')
    
    async with get_db_cursor(dictionary=True) as cursor:
        if action == 'approve':
            # Estado final de aprobación financiera
            await cursor.execute("""
                UPDATE cmp_ordenes_compra 
                SET estado = 'APROBADA_TESORERIA', 
                    fecha_pago_estimada = %s,
                    aprobador_tesoreria_id = %s,
                    fecha_aprobacion_tesoreria = NOW()
                WHERE id = %s AND enterprise_id = %s
            """, (fecha_pago, g.user['id'], id, g.user['enterprise_id']))
            await flash(f"Fondos aprobados para PO #{id}. Pago estimado: {fecha_pago}", "success")
            
        elif action == 'reject':
            # Vuelve a Compras para revisión
            await cursor.execute("""
                UPDATE cmp_ordenes_compra 
                SET estado = 'RECHAZADA_TESORERIA',
                    observaciones_rechazo = %s
                WHERE id = %s AND enterprise_id = %s
            """, (observaciones, id, g.user['enterprise_id']))
            await flash(f"PO #{id} enviada de regreso a Compras para ajuste.", "warning")

    return redirect(url_for('fondos.aprobaciones'))

@fondos_bp.route('/fondos/kpi-desembolsos')
@login_required
async def kpi_desembolsos():
    """KPI de compromisos de fondos por fecha."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT fecha_pago_estimada, SUM(total_estimado) as monto_total, COUNT(*) as po_count
            FROM cmp_ordenes_compra
            WHERE estado = 'APROBADA_TESORERIA' AND enterprise_id = %s
            AND fecha_pago_estimada >= CURDATE()
            GROUP BY fecha_pago_estimada
            ORDER BY fecha_pago_estimada ASC
        """, (g.user['enterprise_id'],))
        calendario = await cursor.fetchall()
    return await render_template('fondos/kpi_desembolsos.html', calendario=calendario)

@fondos_bp.route('/fondos/condiciones-pago', methods=['GET', 'POST'])
@login_required
@atomic_transaction('fondos', severity=5, impact_category='Operational')
async def condiciones_pago():
    async with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            try:
                action = (await request.form).get('action')
                if action == 'create':
                    nombre = (await request.form)['nombre']
                    dias = (await request.form).get('dias_vencimiento', 0)
                    descuento = (await request.form).get('descuento_pct', 0)
                    recargo = (await request.form).get('recargo_pct', 0)
                    
                    await cursor.execute("""
                        INSERT INTO fin_condiciones_pago (enterprise_id, nombre, dias_vencimiento, descuento_pct, recargo_pct, user_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (g.user['enterprise_id'], nombre, dias, descuento, recargo, g.user['id']))
                    await flash('Condición de pago creada', 'success')
                    
                elif action == 'edit':
                    cid = (await request.form)['id']
                    nombre = (await request.form)['nombre']
                    dias = (await request.form).get('dias_vencimiento', 0)
                    descuento = (await request.form).get('descuento_pct', 0)
                    recargo = (await request.form).get('recargo_pct', 0)
                    activo = 1 if 'activo' in (await request.form) else 0
                    
                    await cursor.execute("""
                        UPDATE fin_condiciones_pago 
                        SET nombre=%s, dias_vencimiento=%s, descuento_pct=%s, recargo_pct=%s, activo=%s, user_id_update=%s 
                        WHERE id=%s AND enterprise_id=%s
                    """, (nombre, dias, descuento, recargo, activo, g.user['id'], cid, g.user['enterprise_id']))
                    await flash('Condición de pago actualizada', 'success')
                
                elif action == 'delete':
                    cid = (await request.form)['id']
                    await cursor.execute("DELETE FROM fin_condiciones_pago WHERE id=%s AND enterprise_id=%s", (cid, g.user['enterprise_id']))
                    await flash('Condición de pago eliminada', 'warning')
                    
            except Exception as e:
                await flash(f'Error: {str(e)}', 'danger')
                
            return redirect(url_for('fondos.condiciones_pago'))
            
        # GET
        await cursor.execute("SELECT * FROM fin_condiciones_pago WHERE (enterprise_id = %s OR enterprise_id = 0) ORDER BY dias_vencimiento, nombre", (g.user['enterprise_id'],))
        condiciones = await cursor.fetchall()
        for c in condiciones:
            if c.get('created_at'): c['created_at'] = str(c['created_at'])
            if c.get('updated_at'): c['updated_at'] = str(c['updated_at'])

    return await render_template('fondos/condiciones_pago.html', condiciones=condiciones)

@fondos_bp.route('/fondos/condiciones-mixtas', methods=['GET', 'POST'])
@login_required
@atomic_transaction('fondos', severity=5, impact_category='Operational')
async def condiciones_mixtas():
    async with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            try:
                action = (await request.form).get('action')
                if action == 'create' or action == 'edit':
                    nombre = (await request.form)['nombre']
                    descripcion = (await request.form).get('descripcion', '')
                    condiciones_ids = (await request.form).getlist('condicion_pago_id[]')
                    porcentajes = (await request.form).getlist('porcentaje[]')
                    descuentos = (await request.form).getlist('descuento_pct_det[]')
                    recargos = (await request.form).getlist('recargo_pct_det[]')
                    activo = 1 if 'activo' in (await request.form) else 0
                    
                    if action == 'create':
                        await cursor.execute("""
                            INSERT INTO fin_condiciones_pago_mixtas (enterprise_id, nombre, descripcion, user_id)
                            VALUES (%s, %s, %s, %s)
                        """, (g.user['enterprise_id'], nombre, descripcion, g.user['id']))
                        mixta_id = cursor.lastrowid
                    else:
                        mixta_id = (await request.form)['id']
                        await cursor.execute("""
                            UPDATE fin_condiciones_pago_mixtas 
                            SET nombre=%s, descripcion=%s, activo=%s, user_id_update=%s
                            WHERE id=%s AND enterprise_id=%s
                        """, (nombre, descripcion, activo, g.user['id'], mixta_id, g.user['enterprise_id']))
                        # Limpiar detalles previos para re-insertar
                        await cursor.execute("DELETE FROM fin_condiciones_pago_mixtas_detalle WHERE mixta_id = %s AND enterprise_id = %s", (mixta_id, g.user['enterprise_id']))
                    
                    # Insertar detalles
                    for cid, pct, desc, rec in zip(condiciones_ids, porcentajes, descuentos, recargos):
                        if cid and pct:
                            await cursor.execute("""
                                INSERT INTO fin_condiciones_pago_mixtas_detalle (enterprise_id, mixta_id, condicion_pago_id, porcentaje, descuento_pct, recargo_pct, user_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (g.user['enterprise_id'], mixta_id, cid, pct, desc or 0, rec or 0, g.user['id']))
                    
                    await flash('Condición mixta guardada correctamente', 'success')
                    
                elif action == 'delete':
                    mid = (await request.form)['id']
                    await cursor.execute("DELETE FROM fin_condiciones_pago_mixtas WHERE id=%s AND enterprise_id=%s", (mid, g.user['enterprise_id']))
                    await flash('Condición mixta eliminada', 'warning')
                    
            except Exception as e:
                await flash(f'Error: {str(e)}', 'danger')
                
            return redirect(url_for('fondos.condiciones_mixtas'))
            
        # GET
        # Obtener cabeceras
        await cursor.execute("SELECT * FROM fin_condiciones_pago_mixtas WHERE (enterprise_id = %s OR enterprise_id = 0) ORDER BY nombre", (g.user['enterprise_id'],))
        mixtas = await cursor.fetchall()
        
        # Obtener detalles para cada mixta (opcional, mejor traerlos on-demand o todos y mapear)
        for m in mixtas:
            if m.get('created_at'): m['created_at'] = str(m['created_at'])
            if m.get('updated_at'): m['updated_at'] = str(m['updated_at'])
            await cursor.execute("""
                SELECT fin_condiciones_pago_mixtas_detalle.*, fin_condiciones_pago.nombre as condicion_nombre 
                FROM fin_condiciones_pago_mixtas_detalle
                JOIN fin_condiciones_pago ON fin_condiciones_pago_mixtas_detalle.condicion_pago_id = fin_condiciones_pago.id
                WHERE fin_condiciones_pago_mixtas_detalle.mixta_id = %s AND (fin_condiciones_pago_mixtas_detalle.enterprise_id = %s OR fin_condiciones_pago_mixtas_detalle.enterprise_id = 0)
            """, (m['id'], g.user['enterprise_id']))
            m['detalles'] = await cursor.fetchall()
            for d in m['detalles']:
                if d.get('created_at'): d['created_at'] = str(d['created_at'])
                if d.get('updated_at'): d['updated_at'] = str(d['updated_at'])
            
        # Condiciones simples para el selector
        await cursor.execute("SELECT * FROM fin_condiciones_pago WHERE (enterprise_id = %s OR enterprise_id = 0) AND activo = 1 ORDER BY nombre", (g.user['enterprise_id'],))
        condiciones_simples = await cursor.fetchall()

    return await render_template('fondos/condiciones_mixtas.html', mixtas=mixtas, condiciones_simples=condiciones_simples)

@fondos_bp.route('/fondos/impuestos', methods=['GET', 'POST'])
@login_required
async def impuestos():
    async with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            try:
                action = (await request.form).get('action')
                if action == 'create':
                    nombre = (await request.form)['nombre']
                    descripcion = (await request.form).get('descripcion', '')
                    
                    await cursor.execute("""
                        INSERT INTO sys_impuestos (enterprise_id, nombre, descripcion)
                        VALUES (%s, %s, %s)
                    """, (g.user['enterprise_id'], nombre, descripcion))
                    
                    # REQUERIMIENTO: Insertar en tabla de numeración (si aplica, lo hacemos por consistencia)
                    try:
                        from services.enterprise_init import sync_new_concept_to_all_enterprises
                        await sync_new_concept_to_all_enterprises('IMPUESTO', nombre)
                    except Exception as e:
                        pass

                    await flash('Impuesto creado exitosamente', 'success')
                    
                elif action == 'edit':
                    iid = (await request.form)['id']
                    nombre = (await request.form)['nombre']
                    descripcion = (await request.form).get('descripcion', '')
                    activo = 1 if 'activo' in (await request.form) else 0
                    
                    await cursor.execute("""
                        UPDATE sys_impuestos 
                        SET nombre=%s, descripcion=%s, activo=%s
                        WHERE id=%s AND (enterprise_id=%s OR enterprise_id=0)
                    """, (nombre, descripcion, activo, iid, g.user['enterprise_id']))
                    await flash('Impuesto actualizado', 'success')
                
                elif action == 'delete':
                    iid = (await request.form)['id']
                    await cursor.execute("DELETE FROM sys_impuestos WHERE id=%s AND enterprise_id=%s", (iid, g.user['enterprise_id']))
                    await flash('Impuesto eliminado', 'warning')
                    
            except Exception as e:
                await flash(f'Error: {str(e)}', 'danger')
                
            return redirect(url_for('fondos.impuestos'))
            
        # GET
        await cursor.execute("SELECT * FROM sys_impuestos WHERE enterprise_id = %s OR enterprise_id = 0 ORDER BY nombre", (g.user['enterprise_id'],))
        impuestos_lista = await cursor.fetchall()
        for imp in impuestos_lista:
            if imp.get('created_at'): imp['created_at'] = str(imp['created_at'])
            if imp.get('updated_at'): imp['updated_at'] = str(imp['updated_at'])

    return await render_template('fondos/impuestos.html', impuestos=impuestos_lista)

@fondos_bp.route('/fondos/configuracion-global')
@login_required
async def configuracion_global():
    """Vista unificada para gestionar los parámetros financieros globales de la empresa."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM fin_medios_pago WHERE (enterprise_id = %s OR enterprise_id = 0) ORDER BY nombre", (g.user['enterprise_id'],))
        medios = await cursor.fetchall()
        for m in medios:
            if m.get('created_at'): m['created_at'] = str(m['created_at'])
            if m.get('updated_at'): m['updated_at'] = str(m['updated_at'])
        
        await cursor.execute("SELECT * FROM fin_condiciones_pago WHERE (enterprise_id = %s OR enterprise_id = 0) ORDER BY dias_vencimiento, nombre", (g.user['enterprise_id'],))
        condiciones = await cursor.fetchall()
        for c in condiciones:
            if c.get('created_at'): c['created_at'] = str(c['created_at'])
            if c.get('updated_at'): c['updated_at'] = str(c['updated_at'])
        
        await cursor.execute("SELECT * FROM fin_condiciones_pago_mixtas WHERE (enterprise_id = %s OR enterprise_id = 0) ORDER BY nombre", (g.user['enterprise_id'],))
        mixtas = await cursor.fetchall()
        for m in mixtas:
            if m.get('created_at'): m['created_at'] = str(m['created_at'])
            if m.get('updated_at'): m['updated_at'] = str(m['updated_at'])
        
        await cursor.execute("SELECT * FROM sys_impuestos WHERE (enterprise_id = %s OR enterprise_id = 0) AND activo = 1 ORDER BY nombre", (g.user['enterprise_id'],))
        impuestos_maestros = await cursor.fetchall()
        for imp in impuestos_maestros:
            if imp.get('created_at'): imp['created_at'] = str(imp['created_at'])
            if imp.get('updated_at'): imp['updated_at'] = str(imp['updated_at'])
        
    return await render_template('fondos/configuracion_global.html', medios=medios, condiciones=condiciones, mixtas=mixtas, impuestos_maestros=impuestos_maestros)


# ============================================================
#  ABM DE BANCOS — Integración con API del BCRA
# ============================================================

@fondos_bp.route('/fondos/bancos', methods=['GET', 'POST'])
@login_required
@atomic_transaction('fondos', severity=4, impact_category='Operational')
async def bancos():
    """
    ABM de entidades bancarias.
    - enterprise_id = 0  → datos maestros importados del BCRA (compartidos)
    - enterprise_id = N  → bancos agregados manualmente por la empresa N
    """
    eid = g.user['enterprise_id']

    async with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            action = (await request.form).get('action')
            try:
                if action == 'create':
                    nombre        = (await request.form)['nombre'].strip()
                    tipo_entidad  = (await request.form).get('tipo_entidad', 'CBU').strip()
                    tipo          = (await request.form).get('tipo', '').strip()
                    cuit          = (await request.form).get('cuit', '').strip() or None
                    bic           = (await request.form).get('bic', '').strip() or None
                    telefono      = (await request.form).get('telefono', '').strip() or None
                    web           = (await request.form).get('web', '').strip() or None
                    direccion     = (await request.form).get('direccion', '').strip() or None
                    numero_cuenta = (await request.form).get('numero_cuenta', '').strip() or None

                    await cursor.execute("""
                        INSERT INTO fin_bancos
                            (enterprise_id, tipo_entidad, numero_cuenta, nombre, tipo,
                             cuit, bic, telefono, web, direccion, origen, activo)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'MANUAL', 1)
                    """, (eid, tipo_entidad, numero_cuenta, nombre, tipo,
                           cuit, bic, telefono, web, direccion))
                    nuevo_id = cursor.lastrowid

                    # Crear cuenta analítica automáticamente (1.1.02.XXX o 1.1.03.XXX)
                    cuenta_id = await BCRAService.crear_cuenta_para_banco(
                        cursor, eid, nuevo_id, nombre, tipo_entidad,
                        numero_cuenta=numero_cuenta)
                    prefijo = '1.1.03' if tipo_entidad == 'CVU' else '1.1.02'
                    extra = f' — Cuenta {prefijo}.XXX generada.' if cuenta_id else ''
                    await flash(f'Entidad "{nombre}" creada correctamente.{extra}', 'success')

                elif action == 'edit':
                    bid           = (await request.form)['id']
                    nombre        = (await request.form)['nombre'].strip()
                    tipo_entidad  = (await request.form).get('tipo_entidad', 'CBU').strip()
                    tipo          = (await request.form).get('tipo', '').strip()
                    cuit          = (await request.form).get('cuit', '').strip() or None
                    bic           = (await request.form).get('bic', '').strip() or None
                    telefono      = (await request.form).get('telefono', '').strip() or None
                    web           = (await request.form).get('web', '').strip() or None
                    direccion     = (await request.form).get('direccion', '').strip() or None
                    activo        = 1 if 'activo' in (await request.form) else 0
                    numero_cuenta = (await request.form).get('numero_cuenta', '').strip() or None

                    cuenta_id = await BCRAService.crear_cuenta_para_banco(
                        cursor, eid, bid, nombre, tipo_entidad,
                        numero_cuenta=numero_cuenta)

                    await cursor.execute("""
                        UPDATE fin_bancos
                        SET nombre=%s, tipo_entidad=%s, tipo=%s, cuit=%s, bic=%s,
                            telefono=%s, web=%s, direccion=%s, activo=%s,
                            numero_cuenta=COALESCE(%s, numero_cuenta),
                            cuenta_contable_id=COALESCE(%s, cuenta_contable_id)
                        WHERE id=%s AND (enterprise_id=%s OR enterprise_id=0)
                    """, (nombre, tipo_entidad, tipo, cuit, bic,
                           telefono, web, direccion, activo,
                           numero_cuenta, cuenta_id, bid, eid))
                    await flash('Entidad actualizada correctamente.', 'success')

                elif action == 'delete':
                    bid = (await request.form)['id']
                    # Baja lógica para registros BCRA; baja física solo para registros propios
                    await cursor.execute("SELECT enterprise_id, origen FROM fin_bancos WHERE id=%s", (bid,))
                    row = await cursor.fetchone()
                    if row:
                        if row['enterprise_id'] == 0 or row['origen'] == 'BCRA':
                            # Solo desactivar — es dato maestro global
                            await cursor.execute(
                                "UPDATE fin_bancos SET activo=0 WHERE id=%s", (bid,))
                            await flash('Banco desactivado (dato maestro BCRA).', 'warning')
                        else:
                            await cursor.execute(
                                "DELETE FROM fin_bancos WHERE id=%s AND enterprise_id=%s", (bid, eid))
                            await flash('Banco eliminado.', 'warning')

            except Exception as e:
                await flash(f'Error: {str(e)}', 'danger')

            return redirect(url_for('fondos.bancos'))

        # ── GET ──────────────────────────────────────────────
        await cursor.execute("""
            SELECT fin_bancos.*,
                   cont_plan_cuentas.codigo AS cuenta_codigo,
                   cont_plan_cuentas.nombre AS cuenta_nombre
            FROM fin_bancos
            LEFT JOIN cont_plan_cuentas ON fin_bancos.cuenta_contable_id = cont_plan_cuentas.id
            WHERE (fin_bancos.enterprise_id = %s OR fin_bancos.enterprise_id = 0)
            ORDER BY fin_bancos.tipo_entidad, fin_bancos.nombre ASC
        """, (eid,))
        bancos_lista = await cursor.fetchall()

        # Asegurar serialización JSON (eliminar/convertir objetos datetime)
        for b in bancos_lista:
            if b.get('created_at'): b['created_at'] = str(b['created_at'])
            if b.get('updated_at'): b['updated_at'] = str(b['updated_at'])

        total     = len(bancos_lista)
        activos   = sum(1 for b in bancos_lista if b['activo'])
        cbu_count = sum(1 for b in bancos_lista if b.get('tipo_entidad') == 'CBU')
        cvu_count = sum(1 for b in bancos_lista if b.get('tipo_entidad') == 'CVU')
        bcra_count = sum(1 for b in bancos_lista if b.get('origen') == 'BCRA')

    return await render_template(
        'fondos/bancos.html',
        bancos=bancos_lista,
        total=total,
        activos=activos,
        cbu_count=cbu_count,
        cvu_count=cvu_count,
        bcra_count=bcra_count,
    )


@fondos_bp.route('/fondos/bancos/sincronizar-bcra', methods=['POST'])
@login_required
async def bancos_sincronizar_bcra():
    """Sincroniza entidades bancarias CBU desde la API del BCRA."""
    try:
        await BCRAService.initialize_db()
        stats = await BCRAService.sincronizar_desde_bcra(enterprise_id=0)
        await flash(
            f'Sincronización CBU completada: '
            f'{stats["insertados"]} nuevas, {stats["actualizados"]} actualizadas, '
            f'{stats["cuentas"]} cuentas generadas, {stats["errores"]} errores '
            f'(de {stats["total"]} totales).',
            'success' if stats['errores'] == 0 else 'warning'
        )
    except ConnectionError as e:
        await flash(f'Sin conexión con el BCRA: {e}', 'danger')
    except TimeoutError as e:
        await flash(f'Timeout al conectar con el BCRA: {e}', 'danger')
    except Exception as e:
        await flash(f'Error al sincronizar bancos: {str(e)}', 'danger')
    return redirect(url_for('fondos.bancos'))


@fondos_bp.route('/fondos/bancos/sincronizar-billeteras', methods=['POST'])
@login_required
async def bancos_sincronizar_billeteras():
    """Sincroniza billeteras virtuales CVU desde la API del BCRA (o listado semilla)."""
    try:
        await BCRAService.initialize_db()
        stats = await BCRAService.sincronizar_billeteras(enterprise_id=0)
        await flash(
            f'Sincronización CVU completada: '
            f'{stats["insertados"]} nuevas, {stats["actualizados"]} actualizadas, '
            f'{stats["cuentas"]} cuentas generadas, {stats["errores"]} errores '
            f'(de {stats["total"]} totales).',
            'success' if stats['errores'] == 0 else 'warning'
        )
    except Exception as e:
        await flash(f'Error al sincronizar billeteras: {str(e)}', 'danger')
    return redirect(url_for('fondos.bancos'))


@fondos_bp.route('/fondos/bancos/api/buscar')
@login_required
async def bancos_api_buscar():
    """
    Endpoint JSON para búsqueda de await bancos(usado por select2 / autocomplete).
    Retorna bancos activos que coincidan con `q`.
    """
    q   = request.args.get('q', '').strip()
    eid = g.user['enterprise_id']

    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT id, nombre, tipo, cuit, bic
            FROM fin_bancos
            WHERE (enterprise_id = %s OR enterprise_id = 0)
              AND activo = 1
              AND nombre LIKE %s
            ORDER BY nombre ASC
            LIMIT 30
        """, (eid, f'%{q}%'))
        results = await cursor.fetchall()

    return jsonify(results)
