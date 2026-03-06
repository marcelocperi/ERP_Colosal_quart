from flask import Blueprint, render_template, request, g, flash, redirect, url_for, jsonify
from core.decorators import login_required
from database import get_db_cursor, atomic_transaction
from services.georef_service import GeorefService
from services.billing_service import BillingService
from services.logistics_service import LogisticsService
import datetime
import json
import io
from xhtml2pdf import pisa
from services.email_service import enviar_notificacion_percepcion, _enviar_email, enviar_solicitud_devolucion
from services.tercero_service import TerceroService
from decimal import Decimal
ventas_bp = Blueprint('ventas', __name__, template_folder='templates')

@ventas_bp.route('/ventas/dashboard')
@login_required
def dashboard():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT id, nombre, cuit, codigo
                FROM erp_terceros
                WHERE enterprise_id = %s AND es_cliente = 1
                ORDER BY nombre
            """, (g.user['enterprise_id'],))
            clientes = cursor.fetchall()
        return render_template('ventas/dashboard.html', clientes=clientes)
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f"Error al cargar el dashboard de ventas: {str(e)}", "danger")
        return redirect('/')

@ventas_bp.route('/ventas/cuenta-corriente')
@login_required
def cuenta_corriente_global():
    """Vista global de Cuenta Corriente de todos los clientes con filtros."""
    with get_db_cursor(dictionary=True) as cursor:
        # Lista de clientes para el filtro
        cursor.execute("""
            SELECT id, nombre, cuit, codigo
            FROM erp_terceros
            WHERE enterprise_id = %s AND es_cliente = 1
            ORDER BY nombre
        """, (g.user['enterprise_id'],))
        clientes = cursor.fetchall()

        # Filtros de la URL
        cliente_id   = request.args.get('cliente_id', type=int)
        fecha_desde  = request.args.get('fecha_desde')
        fecha_hasta  = request.args.get('fecha_hasta')
        tipo_doc     = request.args.get('tipo_doc')
        busqueda     = request.args.get('q', '').strip()

        # ── Query unificada ──────────────────────────────────────────
        DEBITO_TIPOS = {'001','002','006','007','011','012','005','010','015'}
        NC_TIPOS     = {'003','008','013'}

        # 1) Comprobantes  — incluye doc. asociado para NC/ND
        comp_sql = """
            SELECT
                erp_terceros.nombre                         AS cliente_nombre,
                erp_terceros.id                             AS cliente_id,
                erp_comprobantes.fecha_emision              AS fecha,
                erp_comprobantes.tipo_comprobante           AS tipo_doc,
                CONCAT(LPAD(erp_comprobantes.punto_venta,4,'0'),'-',
                       LPAD(erp_comprobantes.numero,8,'0')) AS nro_documento,
                NULL                                        AS nro_recibo,
                -- Para NC/ND: número de la factura asociada
                CASE
                    WHEN erp_comprobantes.comprobante_asociado_id IS NOT NULL
                    THEN CONCAT(LPAD(asoc.punto_venta,4,'0'),'-',
                                LPAD(asoc.numero,8,'0'))
                    ELSE NULL
                END                                         AS nro_doc_aplicado,
                erp_comprobantes.comprobante_asociado_id    AS comprobante_asociado_id,
                erp_comprobantes.importe_total              AS importe_bruto,
                erp_comprobantes.tipo_comprobante           AS _signo_tipo,
                erp_comprobantes.id                         AS comprobante_id,
                erp_comprobantes.asiento_id                 AS asiento_id,
                COALESCE((
                    SELECT SUM(erp_comprobantes_impuestos.importe)
                    FROM erp_comprobantes_impuestos
                    WHERE erp_comprobantes_impuestos.comprobante_id = erp_comprobantes.id
                      AND erp_comprobantes_impuestos.enterprise_id  = erp_comprobantes.enterprise_id
                ), 0)                                       AS total_percepciones,
                0                                           AS total_retenciones
            FROM erp_comprobantes
            JOIN erp_terceros ON erp_terceros.id = erp_comprobantes.tercero_id AND erp_terceros.es_cliente = 1
            LEFT JOIN erp_comprobantes AS asoc ON asoc.id = erp_comprobantes.comprobante_asociado_id
            WHERE erp_comprobantes.enterprise_id = %s
              AND erp_comprobantes.modulo IN ('VEN', 'VENTAS')
        """
        comp_params = [g.user['enterprise_id']]
        if cliente_id:
            comp_sql += " AND erp_comprobantes.tercero_id = %s"
            comp_params.append(cliente_id)
        if fecha_desde:
            comp_sql += " AND erp_comprobantes.fecha_emision >= %s"
            comp_params.append(fecha_desde)
        if fecha_hasta:
            comp_sql += " AND erp_comprobantes.fecha_emision <= %s"
            comp_params.append(fecha_hasta)
        if tipo_doc and tipo_doc != 'TODOS':
            if tipo_doc == 'FACTURA':
                comp_sql += " AND erp_comprobantes.tipo_comprobante IN ('001','002','006','007','011','012')"
            elif tipo_doc == 'NC':
                comp_sql += " AND erp_comprobantes.tipo_comprobante IN ('003','008','013')"
            elif tipo_doc == 'ND':
                comp_sql += " AND erp_comprobantes.tipo_comprobante IN ('005','010','015')"
            elif tipo_doc == 'REC':
                comp_sql += " AND 1=0"  # Los recibos no están aquí

        cursor.execute(comp_sql, comp_params)
        rows_comp = cursor.fetchall()

        # 2) Recibos
        rec_sql = """
            SELECT
                erp_terceros.nombre                         AS cliente_nombre,
                erp_terceros.id                             AS cliente_id,
                fin_recibos.fecha                           AS fecha,
                'REC'                                       AS tipo_doc,
                NULL                                        AS nro_documento,
                CONCAT(LPAD(fin_recibos.punto_venta,4,'0'),'-',
                       LPAD(fin_recibos.numero,8,'0'))      AS nro_recibo,
                GROUP_CONCAT(
                    DISTINCT CONCAT(LPAD(erp_comprobantes.punto_venta,4,'0'),'-',LPAD(erp_comprobantes.numero,8,'0'))
                    ORDER BY erp_comprobantes.numero SEPARATOR ' / '
                )                                           AS nro_doc_aplicado,
                SUM(fin_recibos_detalles.importe)           AS importe_bruto,
                'REC'                                       AS _signo_tipo,
                NULL                                        AS comprobante_id,
                NULL                                        AS comprobante_asociado_id,
                fin_recibos.asiento_id                      AS asiento_id,
                0                                           AS total_percepciones,
                COALESCE((
                    SELECT SUM(fin_retenciones_emitidas.importe_retencion)
                    FROM fin_retenciones_emitidas
                    WHERE fin_retenciones_emitidas.comprobante_pago_id = fin_recibos.id
                      AND fin_retenciones_emitidas.enterprise_id = fin_recibos.enterprise_id
                ), 0)                                       AS total_retenciones
            FROM fin_recibos
            JOIN fin_recibos_detalles ON fin_recibos_detalles.recibo_id = fin_recibos.id
            JOIN erp_comprobantes      ON erp_comprobantes.id = fin_recibos_detalles.factura_id
            JOIN erp_terceros          ON erp_terceros.id = fin_recibos.tercero_id AND erp_terceros.es_cliente = 1
            WHERE fin_recibos.enterprise_id = %s
        """
        rec_params = [g.user['enterprise_id']]
        if cliente_id:
            rec_sql += " AND fin_recibos.tercero_id = %s"
            rec_params.append(cliente_id)
        if fecha_desde:
            rec_sql += " AND fin_recibos.fecha >= %s"
            rec_params.append(fecha_desde)
        if fecha_hasta:
            rec_sql += " AND fin_recibos.fecha <= %s"
            rec_params.append(fecha_hasta)

        if tipo_doc == 'REC':
            pass  # incluir
        elif tipo_doc and tipo_doc != 'TODOS':
            rec_sql += " AND 1=0"  # filtrar solo recibos si tipo es otro

        rec_sql += " GROUP BY fin_recibos.id, fin_recibos.fecha, fin_recibos.punto_venta, fin_recibos.numero, fin_recibos.asiento_id, erp_terceros.id, erp_terceros.nombre"
        cursor.execute(rec_sql, rec_params)
        rows_rec = cursor.fetchall()

    # ── Calcular saldo por cliente y ordenar ──────────────────────
    movimientos = []
    saldo_por_cliente = {}

    all_rows = sorted(list(rows_comp) + list(rows_rec),
                      key=lambda r: (r['cliente_id'], r['fecha'] or '1900-01-01'))

    for row in all_rows:
        cid  = row['cliente_id']
        tipo = row['_signo_tipo']
        importe = float(row['importe_bruto'] or 0)

        if cid not in saldo_por_cliente:
            saldo_por_cliente[cid] = 0.0

        if tipo in DEBITO_TIPOS:
            debe  = importe; haber = 0.0
            saldo_por_cliente[cid] += importe
        elif tipo in NC_TIPOS:
            debe  = 0.0; haber = importe
            saldo_por_cliente[cid] -= importe
        else:  # REC
            debe  = 0.0; haber = importe
            saldo_por_cliente[cid] -= importe

        mov = dict(row)
        mov['debe']  = debe
        mov['haber'] = haber
        mov['saldo'] = saldo_por_cliente[cid]
        mov['total_percepciones'] = float(row['total_percepciones'] or 0)
        mov['total_retenciones']  = float(row['total_retenciones']  or 0)
        movimientos.append(mov)

    # ── Búsqueda global texto libre ───────────────────────────────
    if busqueda:
        q = busqueda.lower()
        movimientos = [m for m in movimientos if
                       q in (m['cliente_nombre'] or '').lower() or
                       q in (m['nro_documento'] or '').lower() or
                       q in (m['nro_recibo'] or '').lower() or
                       q in (m['nro_doc_aplicado'] or '').lower()]

    return render_template('ventas/cuenta_corriente_global.html',
                           clientes=clientes,
                           movimientos=movimientos,
                           filtros={
                               'cliente_id':  cliente_id,
                               'fecha_desde': fecha_desde,
                               'fecha_hasta': fecha_hasta,
                               'tipo_doc':    tipo_doc,
                               'q':           busqueda,
                           })

@ventas_bp.route('/api/ventas/afip/consultar/<cuit>')
@login_required
def consultar_afip(cuit):
    import asyncio
    from services.afip_service import AfipService
    res = asyncio.run(AfipService.consultar_padron(g.user['enterprise_id'], cuit))
    return jsonify(res)

@ventas_bp.route('/api/ventas/fiscal/allowed-docs')
@login_required
def get_allowed_docs():
    receptor_tipo = request.args.get('tipo_responsable', '*')
    
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT condicion_iva FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
        emp = cursor.fetchone()
        emisor_tipo = emp['condicion_iva'] if emp else 'Responsable Inscripto'
        
    allowed_codigos = BillingService.get_allowed_comprobantes(emisor_tipo, receptor_tipo)
    
    if not allowed_codigos:
        # Fallback to B types if something is wrong
        allowed_codigos = ['006', '007', '008']
        
    with get_db_cursor(dictionary=True) as cursor:
        placeholders = ', '.join(['%s'] * len(allowed_codigos))
        cursor.execute(f"SELECT codigo, descripcion, letra FROM sys_tipos_comprobante WHERE codigo IN ({placeholders})", tuple(allowed_codigos))
        tipos = cursor.fetchall()
        
    return jsonify(tipos)

# --- GESTION DE CLIENTES ---

@ventas_bp.route('/ventas/clientes/nuevo', methods=['GET', 'POST'])
@login_required
@atomic_transaction('ventas')
def nuevo_cliente():
    if request.method == 'POST':
        codigo = request.form.get('codigo', '')
        nombre = request.form['nombre']
        cuit = request.form['cuit']
        email = request.form['email']
        tipo = request.form['tipo_responsable']
        observaciones = request.form.get('observaciones', '')
        
        from services.validation_service import validar_cuit, format_cuit
        if not validar_cuit(cuit):
            flash("Error: El CUIT ingresado no es válido según las reglas fiscales.", "danger")
            return render_template('ventas/cliente_form.html')
            
        cuit = format_cuit(cuit)

        try:
            with get_db_cursor(dictionary=True) as cursor:
                # Validar duplicados
                cursor.execute("SELECT id FROM erp_terceros WHERE cuit = %s AND enterprise_id = %s", (cuit, g.user['enterprise_id']))
                if cursor.fetchone():
                    flash("Error: Ya existe un tercero con ese CUIT o DNI.", "danger")
                else:
                    # Generar código si no se proveyó
                    if not codigo:
                        codigo = TerceroService.generar_siguiente_codigo(g.user['enterprise_id'], 'CLI')

                    cursor.execute("""
                        INSERT INTO erp_terceros (enterprise_id, codigo, nombre, cuit, email, observaciones, es_cliente, tipo_responsable, naturaleza)
                        VALUES (%s, %s, %s, %s, %s, %s, 1, %s, 'CLI')
                    """, (g.user['enterprise_id'], codigo, nombre, cuit, email, observaciones, tipo))
                    
                    flash(f"Cliente registrado exitosamente con el número {codigo}. Ahora puede completar los detalles.", "success")
                    cursor.execute("SELECT LAST_INSERT_ID() as last_id")
                    new_id = cursor.fetchone()['last_id']
                    
                    # Crear dirección por defecto (Depósito Único)
                    cursor.execute("""
                        INSERT INTO erp_direcciones (enterprise_id, tercero_id, etiqueta, calle, numero, localidad, provincia, es_fiscal, es_entrega)
                        VALUES (%s, %s, 'Casa Central', 'A completar', '0', 'Ciudad', 'Provincia', 1, 1)
                    """, (g.user['enterprise_id'], new_id))
                    
                    return redirect(url_for('ventas.perfil_cliente', id=new_id))
        except Exception as e:
            flash(f"Error: {str(e)}", "danger")
            
    return render_template('ventas/cliente_form.html')

@ventas_bp.route('/ventas/clientes/editar/<int:id>', methods=['POST'])
@login_required
def editar_cliente(id):
    nombre = request.form['nombre']
    cuit = request.form['cuit']
    email = request.form.get('email')
    telefono = request.form.get('telefono')
    observaciones = request.form.get('observaciones')
    tipo_responsable = request.form.get('tipo_responsable')
    codigo = request.form.get('codigo')
    condicion_mixta_id = request.form.get('condicion_mixta_id') or None
    condicion_pago_id = request.form.get('condicion_pago_id') or None
    
    from services.validation_service import validar_cuit, format_cuit
    if not validar_cuit(cuit):
        flash("Error: El CUIT ingresado no es válido.", "danger")
        return redirect(url_for('ventas.perfil_cliente', id=id))

    cuit = format_cuit(cuit)

    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                UPDATE erp_terceros 
                SET codigo=%s, nombre=%s, cuit=%s, email=%s, tipo_responsable=%s, observaciones=%s, 
                    telefono=%s, condicion_mixta_id=%s, condicion_pago_id=%s
                WHERE id=%s AND enterprise_id=%s
            """, (codigo, nombre, cuit, email, tipo_responsable, observaciones, telefono, condicion_mixta_id, condicion_pago_id, id, g.user['enterprise_id']))
            flash("Datos básicos actualizados.", "success")
    except Exception as e:
        flash(f"Error al actualizar: {str(e)}", "danger")
    
    return redirect(url_for('ventas.perfil_cliente', id=id))

@ventas_bp.route('/ventas/clientes')
@login_required
def clientes():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT erp_terceros.*, erp_direcciones.calle, erp_direcciones.numero, erp_direcciones.localidad, erp_direcciones.provincia 
            FROM erp_terceros
            LEFT JOIN erp_direcciones ON erp_terceros.id = erp_direcciones.tercero_id AND erp_direcciones.es_fiscal = 1
            WHERE erp_terceros.enterprise_id = %s AND erp_terceros.es_cliente = 1
            GROUP BY erp_terceros.id
        """, (g.user['enterprise_id'],))
        clientes = cursor.fetchall()
    return render_template('ventas/clientes.html', clientes=clientes)

@ventas_bp.route('/ventas/clientes/perfil/<int:id>')
@login_required
def perfil_cliente(id):
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Datos básicos
            cursor.execute("SELECT * FROM erp_terceros WHERE id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            cliente = cursor.fetchone()
            if not cliente:
                flash("Cliente no encontrado.", "warning")
                return redirect(url_for('ventas.clientes'))
                
            # Direcciones
            cursor.execute("SELECT * FROM erp_direcciones WHERE tercero_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            direcciones = cursor.fetchall()
            
            # Contactos joining with Puestos
            cursor.execute("""
                SELECT erp_contactos.*, erp_puestos.nombre as puesto_nombre, erp_direcciones.etiqueta as direccion_nombre
                FROM erp_contactos
                LEFT JOIN erp_puestos ON erp_contactos.puesto_id = erp_puestos.id
                LEFT JOIN erp_direcciones ON erp_contactos.direccion_id = erp_direcciones.id
                WHERE erp_contactos.tercero_id = %s AND erp_contactos.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            contactos = cursor.fetchall()
            
            # Datos Fiscales
            cursor.execute("SELECT * FROM erp_datos_fiscales WHERE tercero_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            fiscales = cursor.fetchall()
            
            # Condiciones de Pago
            cursor.execute("""
                SELECT erp_terceros.condicion_pago_id, erp_terceros.condicion_pago_pendiente_id, erp_terceros.estado_aprobacion_pago,
                       erp_terceros.condicion_mixta_id, fin_condiciones_pago_mixtas.nombre as mixta_nombre,
                       cp_actual.nombre as condicion_nombre, cp_pendiente.nombre as condicion_pendiente_nombre,
                       sys_users.username as aprobador_nombre, erp_terceros.fecha_aprobacion_pago
                FROM erp_terceros
                LEFT JOIN fin_condiciones_pago AS cp_actual ON erp_terceros.condicion_pago_id = cp_actual.id
                LEFT JOIN fin_condiciones_pago AS cp_pendiente ON erp_terceros.condicion_pago_pendiente_id = cp_pendiente.id
                LEFT JOIN fin_condiciones_pago_mixtas ON erp_terceros.condicion_mixta_id = fin_condiciones_pago_mixtas.id
                LEFT JOIN sys_users ON erp_terceros.id_gerente_aprobador = sys_users.id
                WHERE erp_terceros.id = %s AND erp_terceros.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            pago_info = cursor.fetchone()

            # Todas las condiciones disponibles para el modal
            cursor.execute("SELECT * FROM fin_condiciones_pago WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1 ORDER BY nombre", (g.user['enterprise_id'],))
            condiciones_disponibles = cursor.fetchall()
            
            # Condiciones mixtas disponibles
            cursor.execute("SELECT * FROM fin_condiciones_pago_mixtas WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1 ORDER BY nombre", (g.user['enterprise_id'],))
            mixtas_disponibles = cursor.fetchall()

            # Marcar cuáles están habilitadas y obtener fechas + estado
            cursor.execute("SELECT condicion_pago_id, fecha_habilitacion, habilitado FROM erp_terceros_condiciones WHERE tercero_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            habilitaciones = {r['condicion_pago_id']: {'fecha': r['fecha_habilitacion'], 'habilitado': r['habilitado']} for r in cursor.fetchall()}
            
            # Identificar cuáles son parte de la "Maestra"
            incluidas_en_maestra = []
            if pago_info['condicion_mixta_id']:
                cursor.execute("SELECT condicion_pago_id FROM fin_condiciones_pago_mixtas_detalle WHERE mixta_id = %s AND (enterprise_id = 0 OR enterprise_id = %s)", (pago_info['condicion_mixta_id'], g.user['enterprise_id']))
                incluidas_en_maestra = [r['condicion_pago_id'] for r in cursor.fetchall()]
            elif pago_info['condicion_pago_id']:
                incluidas_en_maestra = [pago_info['condicion_pago_id']]
            
            # Provincias (Georef)
            provincias = GeorefService.get_provincias()

            # Impuestos Maestros (Configurables)
            cursor.execute("SELECT id, nombre FROM sys_impuestos WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1 ORDER BY nombre", (g.user['enterprise_id'],))
            impuestos_lista = cursor.fetchall()
            
            # Coeficientes CM05
            cursor.execute("""
                SELECT erp_terceros_cm05.*, sys_provincias.nombre as provincia_nombre
                FROM erp_terceros_cm05
                LEFT JOIN sys_provincias ON BINARY erp_terceros_cm05.jurisdiccion_code = BINARY LPAD(sys_provincias.id, 3, '0')
                WHERE erp_terceros_cm05.tercero_id = %s AND erp_terceros_cm05.enterprise_id = %s
                ORDER BY erp_terceros_cm05.periodo_anio DESC, erp_terceros_cm05.jurisdiccion_code ASC
            """, (id, g.user['enterprise_id']))
            coeficientes_cm = cursor.fetchall()
            
            # Fallback de nombre de provincia estático si sys_provincias no funciona o id no mapea
            for c in coeficientes_cm:
                if not c.get('provincia_nombre'):
                    c['provincia_nombre'] = f"Jurisdicción {c['jurisdiccion_code']}"

            # ── Cuenta Corriente ──────────────────────────────────────────────
            # 1. Comprobantes: Facturas (débito), NC (crédito), ND (débito)
            #    Percepciones: suma desde erp_comprobantes_impuestos (cubre todas las provincias).
            #    Retenciones en comprobantes = 0 (las retenciones van en el recibo).
            #    asiento_id para el botón de ir al asiento contable.
            cursor.execute("""
                SELECT
                    erp_comprobantes.fecha_emision                      AS fecha,
                    erp_comprobantes.tipo_comprobante                  AS tipo_doc,
                    CONCAT(
                        LPAD(erp_comprobantes.punto_venta, 4, '0'), '-',
                        LPAD(erp_comprobantes.numero,      8, '0')
                    )                                           AS nro_documento,
                    NULL                                        AS nro_recibo,
                    NULL                                        AS nro_doc_aplicado,
                    erp_comprobantes.importe_total                     AS importe_bruto,
                    erp_comprobantes.tipo_comprobante                  AS _signo_tipo,
                    erp_comprobantes.id                                AS comprobante_id,
                    erp_comprobantes.asiento_id                        AS asiento_id,
                    COALESCE((
                        SELECT SUM(erp_comprobantes_impuestos.importe)
                        FROM erp_comprobantes_impuestos
                        WHERE erp_comprobantes_impuestos.comprobante_id = erp_comprobantes.id
                          AND erp_comprobantes_impuestos.enterprise_id  = erp_comprobantes.enterprise_id
                    ), 0)                                       AS total_percepciones,
                    0                                           AS total_retenciones
                FROM erp_comprobantes
                WHERE erp_comprobantes.tercero_id = %s
                  AND erp_comprobantes.enterprise_id = %s
                  AND erp_comprobantes.modulo IN ('VEN', 'VENTAS')
            """, (id, g.user['enterprise_id']))
            rows_comp = cursor.fetchall()

            # 2. Recibos de cobro aplicados a facturas de este cliente
            #    Agrupa por recibo para no duplicar filas si tiene varias facturas aplicadas.
            #    Las retenciones emitidas se vinculan por comprobante_pago_id (= recibo).
            cursor.execute("""
                SELECT
                    fin_recibos.fecha                                     AS fecha,
                    'REC'                                       AS tipo_doc,
                    NULL                                        AS nro_documento,
                    CONCAT(
                        LPAD(fin_recibos.punto_venta, 4, '0'), '-',
                        LPAD(fin_recibos.numero,      8, '0')
                    )                                           AS nro_recibo,
                    GROUP_CONCAT(
                        DISTINCT CONCAT(
                            LPAD(erp_comprobantes.punto_venta, 4, '0'), '-',
                            LPAD(erp_comprobantes.numero,      8, '0')
                        )
                        ORDER BY erp_comprobantes.numero
                        SEPARATOR ' / '
                    )                                           AS nro_doc_aplicado,
                    SUM(fin_recibos_detalles.importe)                             AS importe_bruto,
                    'REC'                                       AS _signo_tipo,
                    NULL                                        AS comprobante_id,
                    fin_recibos.asiento_id                                AS asiento_id,
                    0                                           AS total_percepciones,
                    COALESCE((
                        SELECT SUM(fin_retenciones_emitidas.importe_retencion)
                        FROM fin_retenciones_emitidas
                        WHERE fin_retenciones_emitidas.comprobante_pago_id = fin_recibos.id
                          AND fin_retenciones_emitidas.enterprise_id = fin_recibos.enterprise_id
                    ), 0)                                       AS total_retenciones
                FROM fin_recibos
                JOIN fin_recibos_detalles ON fin_recibos_detalles.recibo_id = fin_recibos.id
                JOIN erp_comprobantes ON erp_comprobantes.id = fin_recibos_detalles.factura_id
                WHERE fin_recibos.tercero_id = %s
                  AND fin_recibos.enterprise_id = %s
                GROUP BY fin_recibos.id, fin_recibos.fecha, fin_recibos.punto_venta, fin_recibos.numero, fin_recibos.asiento_id
            """, (id, g.user['enterprise_id']))
            rows_rec = cursor.fetchall()

            # 3. Unir y ordenar por fecha
            # Saldo: débito en facturas/ND, crédito en NC/recibos
            # Percepciones y retenciones son solo informativas — no afectan el saldo.
            DEBITO_TIPOS = {'001','002','006','007','011','012','005','010','015'}
            NC_TIPOS     = {'003','008','013'}

            cuenta_corriente = []
            saldo = 0.0
            for row in sorted(list(rows_comp) + list(rows_rec),
                              key=lambda r: (r['fecha'] or '1900-01-01')):
                tipo = row['_signo_tipo']
                importe = float(row['importe_bruto'] or 0)

                if tipo in DEBITO_TIPOS:
                    debe    = importe
                    haber   = 0.0
                    saldo  += importe
                elif tipo in NC_TIPOS:
                    debe    = 0.0
                    haber   = importe
                    saldo  -= importe
                else:  # REC — cobro
                    debe    = 0.0
                    haber   = importe
                    saldo  -= importe

                cuenta_corriente.append({
                    'fecha':              row['fecha'],
                    'tipo_doc':           row['tipo_doc'],
                    'nro_documento':      row['nro_documento'],
                    'nro_recibo':         row['nro_recibo'],
                    'nro_doc_aplicado':   row['nro_doc_aplicado'],
                    'debe':               debe,
                    'haber':              haber,
                    'saldo':              saldo,
                    'comprobante_id':     row['comprobante_id'],
                    'asiento_id':         row['asiento_id'],
                    'total_percepciones': float(row['total_percepciones'] or 0),
                    'total_retenciones':  float(row['total_retenciones'] or 0),
                })

            # ── Track Cuenta Corriente ─────────────────────────────────────────
            cursor.execute("""
                SELECT erp_terceros_cta_cte_track.*,
                       sys_users.username AS user_nombre
                FROM erp_terceros_cta_cte_track
                LEFT JOIN sys_users ON sys_users.id = erp_terceros_cta_cte_track.user_id
                WHERE erp_terceros_cta_cte_track.tercero_id = %s
                  AND erp_terceros_cta_cte_track.enterprise_id = %s
                ORDER BY erp_terceros_cta_cte_track.fecha_vigencia DESC
                LIMIT 20
            """, (id, g.user['enterprise_id']))
            cta_cte_track = cursor.fetchall()

        return render_template('ventas/perfil_cliente.html',
                               cliente=cliente,
                               direcciones=direcciones,
                               contactos=contactos,
                               fiscales=fiscales,
                               pago_info=pago_info,
                               condiciones=condiciones_disponibles,
                               mixtas=mixtas_disponibles,
                               habilitadas=habilitaciones,
                               incluidas_en_maestra=incluidas_en_maestra,
                               provincias=provincias,
                               impuestos_lista=impuestos_lista,
                               cuenta_corriente=cuenta_corriente,
                               coeficientes_cm=coeficientes_cm,
                               cta_cte_track=cta_cte_track)
    except Exception as e:
        flash(f"Falla Interna (Premium): {str(e)}", "danger")
        return redirect(url_for('ventas.clientes'))

@ventas_bp.route('/ventas/clientes/solicitar-cta-cte/<int:id>', methods=['POST'])
@login_required
def solicitar_cta_cte(id):
    """Encola un cambio de Cuenta Corriente para aprobación de Tesorería.
    Registra el log en erp_terceros_cta_cte_track con estado PENDIENTE.
    """
    habilita = 1 if request.form.get('habilita_cta_cte') else 0
    monto_raw = request.form.get('monto_cta_cte', '0') or '0'
    try:
        monto = float(monto_raw)
    except ValueError:
        monto = 0.0

    motivo = request.form.get('motivo', '')
    enterprise_id = g.user['enterprise_id']
    user_id = g.user['id']

    try:
        with get_db_cursor() as cursor:
            # Guardar solicitud pendiente en erp_terceros - columnas auxiliares de pendiente
            cursor.execute("""
                UPDATE erp_terceros
                SET cta_cte_pendiente_habilita  = %s,
                    cta_cte_pendiente_monto     = %s,
                    estado_cta_cte_aprobacion   = 'PENDIENTE',
                    cta_cte_pendiente_user_id   = %s
                WHERE id = %s AND enterprise_id = %s
            """, (habilita, monto, user_id, id, enterprise_id))

            # Log en track con estado PENDIENTE
            cursor.execute("""
                INSERT INTO erp_terceros_cta_cte_track
                    (enterprise_id, tercero_id, habilita_cta_cte, monto_cta_cte,
                     estado, motivo, user_id, fecha_vigencia)
                VALUES (%s, %s, %s, %s, 'PENDIENTE', %s, %s, NOW())
            """, (enterprise_id, id, habilita, monto, motivo, user_id))

        flash("Solicitud de Cuenta Corriente enviada a Tesorería para aprobación.", "info")
        return redirect(url_for('ventas.perfil_cliente', id=id))
    except Exception as e:
        flash(f"Falla Interna (Premium): Solicitud fallida. {str(e)}", "danger")
        return redirect(url_for('ventas.perfil_cliente', id=id))


@ventas_bp.route('/ventas/clientes/aprobar-cta-cte/<int:id>', methods=['POST'])
@login_required
def aprobar_cta_cte(id):
    """Aprueba o rechaza el cambio de Cuenta Corriente pendiente.
    Solo usuarios con permiso 'tesoreria', 'admin' o 'all' pueden ejecutar esta acción.
    """
    is_tesoreria = ('tesoreria' in g.permissions or
                    'admin' in g.permissions or
                    'all' in g.permissions)
    if not is_tesoreria:
        flash("No tiene permisos para aprobar cambios de Cuenta Corriente.", "danger")
        return redirect(url_for('ventas.perfil_cliente', id=id))

    action = request.form.get('action')  # 'approve' or 'reject'
    enterprise_id = g.user['enterprise_id']
    user_id = g.user['id']

    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Leer los valores pendientes
            cursor.execute("""
                SELECT cta_cte_pendiente_habilita, cta_cte_pendiente_monto
                FROM erp_terceros
                WHERE id = %s AND enterprise_id = %s
            """, (id, enterprise_id))
            row = cursor.fetchone()

            if not row:
                flash("Cliente no encontrado.", "warning")
                return redirect(url_for('ventas.perfil_cliente', id=id))

            nuevo_habilita = row['cta_cte_pendiente_habilita']
            nuevo_monto    = row['cta_cte_pendiente_monto']

            if action == 'approve':
                # Aplicar el cambio al campo real
                cursor.execute("""
                    UPDATE erp_terceros
                    SET habilita_cta_cte             = %s,
                        monto_cta_cte                = %s,
                        estado_cta_cte_aprobacion    = 'APROBADO',
                        cta_cte_aprobador_id         = %s,
                        cta_cte_fecha_aprobacion     = NOW(),
                        cta_cte_pendiente_habilita   = NULL,
                        cta_cte_pendiente_monto      = NULL,
                        cta_cte_pendiente_user_id    = NULL
                    WHERE id = %s AND enterprise_id = %s
                """, (nuevo_habilita, nuevo_monto, user_id, id, enterprise_id))

                # Actualizar el último track pendiente a APROBADO
                cursor.execute("""
                    UPDATE erp_terceros_cta_cte_track
                    SET estado = 'APROBADO', aprobador_id = %s, fecha_aprobacion = NOW()
                    WHERE tercero_id = %s AND enterprise_id = %s
                      AND estado = 'PENDIENTE'
                    ORDER BY fecha_vigencia DESC LIMIT 1
                """, (user_id, id, enterprise_id))

                flash("Cuenta Corriente aprobada y aplicada al cliente.", "success")

            else:  # reject
                cursor.execute("""
                    UPDATE erp_terceros
                    SET estado_cta_cte_aprobacion  = 'RECHAZADO',
                        cta_cte_pendiente_habilita  = NULL,
                        cta_cte_pendiente_monto     = NULL,
                        cta_cte_pendiente_user_id   = NULL
                    WHERE id = %s AND enterprise_id = %s
                """, (id, enterprise_id))

                cursor.execute("""
                    UPDATE erp_terceros_cta_cte_track
                    SET estado = 'RECHAZADO', aprobador_id = %s, fecha_aprobacion = NOW()
                    WHERE tercero_id = %s AND enterprise_id = %s
                      AND estado = 'PENDIENTE'
                    ORDER BY fecha_vigencia DESC LIMIT 1
                """, (user_id, id, enterprise_id))

                flash("Solicitud de Cuenta Corriente rechazada.", "warning")

        return redirect(url_for('ventas.perfil_cliente', id=id))
    except Exception as e:
        flash(f"Falla Interna (Premium): Aprobación fallida. {str(e)}", "danger")
        return redirect(url_for('ventas.perfil_cliente', id=id))


@ventas_bp.route('/ventas/clientes/toggle-convenio/<int:id>', methods=['POST'])
@login_required
def toggle_convenio(id):
    es_convenio = 1 if 'es_convenio' in request.form else 0
    with get_db_cursor() as cursor:
        cursor.execute("UPDATE erp_terceros SET es_convenio_multilateral = %s WHERE id = %s AND enterprise_id = %s", (es_convenio, id, g.user['enterprise_id']))
    flash("Configuración de convenio multilateral actualizada.", "success")
    return redirect(url_for('ventas.perfil_cliente', id=id))

from services.cm05_service import CM05Service

@ventas_bp.route('/ventas/clientes/agregar-cm05/<int:id>', methods=['POST'])
@login_required
def agregar_cm05(id):
    jurisdiccion = request.form['jurisdiccion_code']
    periodo_anio = request.form['periodo_anio']
    coeficiente = request.form['coeficiente']

    try:
        CM05Service.upsert_coeficiente(g.user['enterprise_id'], id, jurisdiccion, periodo_anio, coeficiente, g.user['id'])
        flash("Coeficiente guardado correctamente.", "success")
    except Exception as e:
        flash(f"Error al guardar coeficiente: {e}", "danger")
    
    return redirect(url_for('ventas.perfil_cliente', id=id))

import os
from werkzeug.utils import secure_filename

@ventas_bp.route('/ventas/clientes/upload-cm05/<int:id>', methods=['POST'])
@login_required
def upload_cm05(id):
    if 'archivo_cm05' not in request.files:
        flash('No se seleccionó ningún archivo.', 'warning')
        return redirect(url_for('ventas.perfil_cliente', id=id))
        
    file = request.files['archivo_cm05']
    if file.filename == '':
        flash('No se seleccionó ningún archivo.', 'warning')
        return redirect(url_for('ventas.perfil_cliente', id=id))
        
    if file and file.filename.lower().endswith('.pdf'):
        filename = secure_filename(f"CM05_{g.user['enterprise_id']}_{id}_{file.filename}")
        upload_folder = os.path.join(os.getcwd(), 'static', 'uploads', 'cm05')
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, filename)
        file.save(file_path)
        
        # Guardar solo la ruta relativa
        rel_path = f"uploads/cm05/{filename}"
        
        with get_db_cursor() as cursor:
            try:
                cursor.execute("UPDATE erp_terceros SET archivo_cm05_path = %s WHERE id = %s", (rel_path, id))
            except BaseException:
                # Add column if it doesn't exist
                cursor.execute("ALTER TABLE erp_terceros ADD COLUMN archivo_cm05_path VARCHAR(255) NULL")
                cursor.execute("UPDATE erp_terceros SET archivo_cm05_path = %s WHERE id = %s", (rel_path, id))
        
        flash("Archivo subido correctamente.", "success")
    else:
        flash("Formato de archivo inválido. Solo PDF.", "danger")
        
    return redirect(url_for('ventas.perfil_cliente', id=id))

@ventas_bp.route('/api/ventas/cliente/<int:id>/logistica')
@login_required
def api_cliente_logistica(id):
    with get_db_cursor(dictionary=True) as cursor:
        # Direcciones de entrega
        cursor.execute("SELECT id, etiqueta, calle, numero, localidad, provincia FROM erp_direcciones WHERE tercero_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
        direcciones = cursor.fetchall()
        
        # Contactos Receptores (incluyendo direccion_id para cascade)
        cursor.execute("SELECT id, nombre, puesto, direccion_id FROM erp_contactos WHERE tercero_id = %s AND enterprise_id = %s AND (es_receptor = 1 OR tipo_contacto = 'LOGISTICA')", (id, g.user['enterprise_id']))
        receptores = cursor.fetchall()
        
    return jsonify({
        'direcciones': direcciones,
        'receptores': receptores
    })

@ventas_bp.route('/api/ventas/cliente/<int:id>/finanzas')
@login_required
def api_cliente_finanzas(id):
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT erp_terceros.condicion_pago_id, fin_condiciones_pago.nombre as condicion_nombre, fin_condiciones_pago.dias_vencimiento, fin_condiciones_pago.descuento_pct
            FROM erp_terceros
            LEFT JOIN fin_condiciones_pago ON erp_terceros.condicion_pago_id = fin_condiciones_pago.id
            WHERE erp_terceros.id = %s AND erp_terceros.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        data = cursor.fetchone() or {}

        # Datos Fiscales (Percepciones)
        cursor.execute("""
            SELECT jurisdiccion, alicuota 
            FROM erp_datos_fiscales 
            WHERE tercero_id = %s AND enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        data['datos_fiscales'] = cursor.fetchall()

    return jsonify(data)

@ventas_bp.route('/api/ventas/cliente/<int:id>/saldo')
@login_required
def api_cliente_saldo(id):
    """Retorna el saldo actual de la cuenta corriente y su límite fijado."""
    incident_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Obtener configuración de Cta Cte del cliente y solicitudes pendientes
            cursor.execute("""
                SELECT 
                    monto_cta_cte, 
                    habilita_cta_cte,
                    estado_cta_cte_aprobacion,
                    cta_cte_pendiente_monto
                FROM erp_terceros 
                WHERE id = %s AND enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            cliente = cursor.fetchone()
            
            monto_cta_cte = float(cliente['monto_cta_cte']) if cliente and cliente['monto_cta_cte'] else 0.0
            habilita = cliente['habilita_cta_cte'] if cliente else 0
            
            # Datos pendientes
            estado_pendiente = cliente['estado_cta_cte_aprobacion'] if cliente else None
            monto_pendiente = float(cliente['cta_cte_pendiente_monto']) if cliente and cliente['cta_cte_pendiente_monto'] else 0.0

            # Definición de tipos para el cálculo del saldo
            DEBITO_TIPOS = "('001','002','006','007','011','012','005','010','015')"
            NC_TIPOS     = "('003','008','013')"
            
            # Debitos - Creditos en comprobantes
            cursor.execute(f"""
                SELECT 
                    COALESCE(SUM(CASE WHEN tipo_comprobante IN {DEBITO_TIPOS} THEN importe_total ELSE 0 END), 0) -
                    COALESCE(SUM(CASE WHEN tipo_comprobante IN {NC_TIPOS} THEN importe_total ELSE 0 END), 0) AS saldo_comp
                FROM erp_comprobantes 
                WHERE tercero_id = %s AND enterprise_id = %s AND modulo IN ('VEN', 'VENTAS')
            """, (id, g.user['enterprise_id']))
            comp_res = cursor.fetchone()
            saldo_comp = float(comp_res['saldo_comp']) if comp_res else 0.0
            
            # Recibos (Creditos)
            cursor.execute("""
                SELECT COALESCE(SUM(importe), 0) as saldo_recibos 
                FROM fin_recibos_detalles rd 
                JOIN fin_recibos r ON rd.recibo_id = r.id 
                WHERE r.tercero_id = %s AND r.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            rec_res = cursor.fetchone()
            saldo_rec = float(rec_res['saldo_recibos']) if rec_res else 0.0
            
            saldo_total = saldo_comp - saldo_rec
            
        return jsonify({
            'success': True,
            'saldo': saldo_total, 
            'monto_cta_cte': monto_cta_cte,
            'habilita_cta_cte': habilita,
            'estado_pendiente': estado_pendiente,
            'monto_pendiente': monto_pendiente
        })
    except Exception as e:
        print(f"ERROR api_cliente_saldo [INC-{incident_id}]: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'incident_id': f"INC-{incident_id}"
        }), 500

@ventas_bp.route('/ventas/clientes/agregar-direccion/<int:id>', methods=['POST'])
@login_required
def agregar_direccion(id):
    item_id = request.form.get('item_id') # Si viene, es edición
    etiqueta = request.form['etiqueta']
    calle = request.form['calle']
    numero = request.form['numero']
    piso = request.form.get('piso', '')
    depto = request.form.get('depto', '')
    localidad = request.form['localidad']
    provincia = request.form['provincia']
    cp = request.form['cod_postal']
    es_fiscal = 1 if 'es_fiscal' in request.form else 0
    es_entrega = 1 if 'es_entrega' in request.form else 0
    with get_db_cursor(dictionary=True) as cursor:
        if item_id:
            cursor.execute("""
                UPDATE erp_direcciones 
                SET etiqueta=%s, calle=%s, numero=%s, piso=%s, depto=%s, localidad=%s, provincia=%s, cod_postal=%s, es_fiscal=%s, es_entrega=%s
                WHERE id=%s AND tercero_id=%s AND enterprise_id=%s
            """, (etiqueta, calle, numero, piso, depto, localidad, provincia, cp, es_fiscal, es_entrega, item_id, id, g.user['enterprise_id']))
            flash("Dirección actualizada.", "success")
        else:
            cursor.execute("""
                INSERT INTO erp_direcciones (enterprise_id, tercero_id, etiqueta, calle, numero, piso, depto, localidad, provincia, cod_postal, es_fiscal, es_entrega)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (g.user['enterprise_id'], id, etiqueta, calle, numero, piso, depto, localidad, provincia, cp, es_fiscal, es_entrega))
            flash("Dirección agregada.", "success")
    
    return redirect(url_for('ventas.perfil_cliente', id=id))

@ventas_bp.route('/ventas/clientes/agregar-contacto/<int:id>', methods=['POST'])
@login_required
def agregar_contacto(id):
    item_id = request.form.get('item_id')
    nombre = request.form['nombre']
    puesto = request.form['puesto']
    tipo = request.form['tipo_contacto']
    telefono = request.form['telefono']
    email = request.form['email']
    es_receptor = 1 if 'es_receptor' in request.form else 0
    direccion_id = request.form.get('direccion_id') or None

    with get_db_cursor(dictionary=True) as cursor:
        # Resolver puesto a ID si existe
        cursor.execute("SELECT id FROM erp_puestos WHERE nombre = %s AND enterprise_id = %s LIMIT 1", (puesto, g.user['enterprise_id']))
        puesto_row = cursor.fetchone()
        puesto_id = puesto_row['id'] if puesto_row else None

        if item_id:
            cursor.execute("""
                UPDATE erp_contactos SET nombre=%s, puesto_id=%s, tipo_contacto=%s, telefono=%s, email=%s, es_receptor=%s, direccion_id=%s
                WHERE id=%s AND tercero_id=%s AND enterprise_id=%s
            """, (nombre, puesto_id, tipo, telefono, email, es_receptor, direccion_id, item_id, id, g.user['enterprise_id']))
            flash("Contacto actualizado.", "success")
        else:
            cursor.execute("""
                INSERT INTO erp_contactos (enterprise_id, tercero_id, nombre, puesto_id, tipo_contacto, telefono, email, es_receptor, direccion_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (g.user['enterprise_id'], id, nombre, puesto_id, tipo, telefono, email, es_receptor, direccion_id))
            flash("Contacto agregado.", "success")
    
    return redirect(url_for('ventas.perfil_cliente', id=id))

@ventas_bp.route('/ventas/clientes/agregar-fiscal/<int:id>', methods=['POST'])
@login_required
def agregar_fiscal(id):
    item_id = request.form.get('item_id')
    impuesto = request.form['impuesto']
    jurisdiccion = request.form['jurisdiccion']
    condicion = request.form['condicion']
    alicuota_raw = request.form.get('alicuota', 0)
    inscripcion = request.form.get('numero_inscripcion', '')
    # Validar alícuota numérica
    try: alicuota = float(alicuota_raw)
    except: alicuota = 0

    with get_db_cursor(dictionary=True) as cursor:
        if item_id:
            cursor.execute("""
                UPDATE erp_datos_fiscales SET impuesto=%s, jurisdiccion=%s, condicion=%s, numero_inscripcion=%s, alicuota=%s
                WHERE id=%s AND tercero_id=%s AND enterprise_id=%s
            """, (impuesto, jurisdiccion, condicion, inscripcion, alicuota, item_id, id, g.user['enterprise_id']))
            flash("Dato fiscal actualizado.", "success")
        else:
            cursor.execute("""
                INSERT INTO erp_datos_fiscales (enterprise_id, tercero_id, impuesto, jurisdiccion, condicion, numero_inscripcion, alicuota)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (g.user['enterprise_id'], id, impuesto, jurisdiccion, condicion, inscripcion, alicuota))
            flash("Dato fiscal agregado.", "success")
    
    return redirect(url_for('ventas.perfil_cliente', id=id))

@ventas_bp.route('/ventas/clientes/eliminar-detalle/<string:tabla>/<int:item_id>/<int:cliente_id>')
@login_required
def eliminar_detalle(tabla, item_id, cliente_id):
    tablas_permitidas = ['erp_direcciones', 'erp_contactos', 'erp_datos_fiscales', 'erp_terceros_cm05']
    if tabla not in tablas_permitidas:
        flash("Operación no permitida.", "danger")
        return redirect(url_for('ventas.perfil_cliente', id=cliente_id))

    if tabla == 'erp_terceros_cm05':
        from services.cm05_service import CM05Service
        CM05Service.delete_coeficiente(g.user['enterprise_id'], item_id, g.user['id'])
    else:
        with get_db_cursor(dictionary=True) as cursor:
            # Verificar que el item pertenezca al tercero y que el tercero sea de la empresa correcta
            cursor.execute(f"DELETE FROM {tabla} WHERE id = %s AND tercero_id = %s AND enterprise_id = %s", (item_id, cliente_id, g.user['enterprise_id']))
        
    flash("Registro eliminado.", "info")
    return redirect(url_for('ventas.perfil_cliente', id=cliente_id))

@ventas_bp.route('/ventas/clientes/solicitar-condicion/<int:id>', methods=['POST'])
@login_required
def solicitar_condicion_pago(id):
    condicion_id = request.form.get('condicion_id')
    if not condicion_id:
        flash("Debe seleccionar una condición.", "warning")
        return redirect(url_for('ventas.perfil_cliente', id=id))
    
    with get_db_cursor() as cursor:
        cursor.execute("""
            UPDATE erp_terceros 
            SET condicion_pago_pendiente_id = %s, estado_aprobacion_pago = 'PENDIENTE'
            WHERE id = %s AND enterprise_id = %s
        """, (condicion_id, id, g.user['enterprise_id']))
        flash("Solicitud de cambio de condición enviada para aprobación del Gerente.", "info")
        
    return redirect(url_for('ventas.perfil_cliente', id=id))

@ventas_bp.route('/ventas/clientes/aprobar-condicion/<int:id>', methods=['POST'])
@login_required
def aprobar_condicion_pago(id):
    # Solo gerentes o admins pueden aprobar
    is_manager = 'gerente_ventas' in g.permissions or 'admin' in g.permissions or 'all' in g.permissions
    if not is_manager:
        flash("No tiene permisos para aprobar condiciones de pago.", "danger")
        return redirect(url_for('ventas.perfil_cliente', id=id))
        
    action = request.form.get('action') # 'approve' or 'reject'
    
    with get_db_cursor() as cursor:
        if action == 'approve':
            cursor.execute("""
                UPDATE erp_terceros 
                SET condicion_pago_id = condicion_pago_pendiente_id,
                    condicion_pago_pendiente_id = NULL,
                    estado_aprobacion_pago = 'APROBADO',
                    id_gerente_aprobador = %s,
                    fecha_aprobacion_pago = NOW()
                WHERE id = %s AND enterprise_id = %s
            """, (g.user['id'], id, g.user['enterprise_id']))
            flash("Nueva condición de pago aprobada y aplicada.", "success")
        else:
            cursor.execute("""
                UPDATE erp_terceros 
                SET condicion_pago_pendiente_id = NULL,
                    estado_aprobacion_pago = 'RECHAZADO'
                WHERE id = %s AND enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            flash("Cambio de condición rechazado.", "warning")
            
    return redirect(url_for('ventas.perfil_cliente', id=id))

@ventas_bp.route('/ventas/clientes/habilitar-condiciones/<int:id>', methods=['POST'])
@login_required
def habilitar_condiciones_pago(id):
    """Habilita o deshabilita condiciones de pago para un cliente específico usando lógica de sobrescritura."""
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Obtener todas las condiciones para saber qué buscar en el form
        cursor.execute("SELECT id FROM fin_condiciones_pago WHERE enterprise_id = %s AND activo = 1", (g.user['enterprise_id'],))
        todas = cursor.fetchall()

        # 2. Limpiar rehabilitaciones previas (sobrescribimos la configuración específica)
        cursor.execute("DELETE FROM erp_terceros_condiciones WHERE tercero_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
        
        # 3. Insertar nuevos estados
        for c in todas:
            cid = c['id']
            # Buscamos 'habilitado_ID' en el form
            estado = request.form.get(f'habilitado_{cid}')
            if estado is not None:
                cursor.execute("""
                    INSERT INTO erp_terceros_condiciones (enterprise_id, tercero_id, condicion_pago_id, habilitado, fecha_habilitacion)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (g.user['enterprise_id'], id, cid, int(estado)))
                
    flash("Configuración de condiciones de pago actualizada.", "success")
    return redirect(url_for('ventas.perfil_cliente', id=id))

@ventas_bp.route('/api/ventas/cliente/<int:id>/condiciones')
@login_required
def get_cliente_condiciones(id):
    """Retorna las condiciones de pago habilitadas para un cliente.
    Lógica: Maestro + Admitidas - Bloqueadas.
    """
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Obtener IDs de condiciones maestras (simple y mixta)
        cursor.execute("SELECT condicion_pago_id, condicion_mixta_id FROM erp_terceros WHERE id = %s", (id,))
        res = cursor.fetchone()
        master_id = res['condicion_pago_id'] if res else None
        mixta_id = res['condicion_mixta_id'] if res else None

        ids_maestra = set()
        if mixta_id:
            # Mixtas pueden estar en 0 (global) o en la específica
            cursor.execute("SELECT condicion_pago_id FROM fin_condiciones_pago_mixtas_detalle WHERE mixta_id = %s AND (enterprise_id = 0 OR enterprise_id = %s)", (mixta_id, g.user['enterprise_id']))
            ids_maestra = {r['condicion_pago_id'] for r in cursor.fetchall()}
        elif master_id:
            ids_maestra = {master_id}

        # 2. Obtener Overrides específicos
        # Los overrides son SIEMPRE de la empresa actual, pero si la empresa es 0, se busca en 0.
        cursor.execute("SELECT condicion_pago_id, habilitado FROM erp_terceros_condiciones WHERE tercero_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
        overrides = {r['condicion_pago_id']: r['habilitado'] for r in cursor.fetchall()}

        condiciones_finales = []

        # 3. Sumar condición mixta si existe (la mixta es una estructura sagrada, no se filtra internamente)
        if mixta_id:
            cursor.execute("SELECT id, nombre, 1 as is_mixed FROM fin_condiciones_pago_mixtas WHERE id = %s AND (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1", (mixta_id, g.user['enterprise_id']))
            mixta_cond = cursor.fetchone()
            if mixta_cond:
                cursor.execute("""
                    SELECT d.porcentaje, cp.nombre as condicion_nombre, cp.dias_vencimiento, cp.descuento_pct, cp.recargo_pct
                    FROM fin_condiciones_pago_mixtas_detalle d
                    JOIN fin_condiciones_pago cp ON d.condicion_pago_id = cp.id
                    WHERE d.mixta_id = %s AND (d.enterprise_id = 0 OR d.enterprise_id = %s)
                """, (mixta_id, g.user['enterprise_id']))
                mixta_cond['detalles'] = cursor.fetchall()
                condiciones_finales.append(mixta_cond)

        # 4. Obtener Simples habilitadas (por maestra o por override)
        cursor.execute("""
            SELECT id, nombre, dias_vencimiento, descuento_pct, recargo_pct, 0 as is_mixed 
            FROM fin_condiciones_pago 
            WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1
            ORDER BY nombre
        """, (g.user['enterprise_id'],))
        todas_simples = cursor.fetchall()

        for s in todas_simples:
            sid = s['id']
            override = overrides.get(sid)
            
            if override == 1: # Explícitamente habilitada
                condiciones_finales.append(s)
            elif override == 0: # Explícitamente bloqueada
                continue
            elif sid in ids_maestra: # Sin override, pero parte de la maestra
                condiciones_finales.append(s)

    return jsonify(condiciones_finales)


# --- FACTURACION ---

@ventas_bp.route('/api/ventas/articulos/buscar')
@login_required
def buscar_articulos():
    naturaleza = request.args.get('naturaleza', '')
    query = request.args.get('q', '').strip()
    ent_id = g.user['enterprise_id']
    
    from utils.barcode_parser import parse_dynamic_barcode
    
    with get_db_cursor(dictionary=True) as cursor:
        # FASE 2.2: Intento de parseo dinámico (Balanza)
        parsed = parse_dynamic_barcode(query, ent_id, cursor)
        search_query = query
        found_dynamic = False
        dynamic_value = None

        if parsed:
            search_query = parsed['sku_plu']
            found_dynamic = True
            dynamic_value = parsed['valor']

        sql = """
            SELECT stk_articulos.id, stk_articulos.nombre, stk_articulos.precio_venta AS precio, 
                   stk_tipos_articulo.naturaleza, stk_articulos.codigo, stk_articulos.unidad_medida,
                   COALESCE(stk_articulos_codigos.codigo, '') AS sku_proveedor
            FROM stk_articulos
            LEFT JOIN stk_tipos_articulo ON stk_articulos.tipo_articulo_id = stk_tipos_articulo.id
            LEFT JOIN stk_articulos_codigos ON stk_articulos.id = stk_articulos_codigos.articulo_id 
                 AND stk_articulos_codigos.enterprise_id = stk_articulos.enterprise_id
            WHERE stk_articulos.enterprise_id = %s AND stk_articulos.activo = 1
        """
        params = [ent_id]
        
        if naturaleza:
            sql += " AND stk_tipos_articulo.naturaleza = %s"
            params.append(naturaleza)
            
        if search_query:
            if found_dynamic:
                # Búsqueda exacta si viene de parseador
                sql += " AND (stk_articulos.codigo = %s OR stk_articulos_codigos.codigo = %s)"
                params.extend([search_query, search_query])
            else:
                sql += """ AND (stk_articulos.nombre LIKE %s OR stk_articulos.codigo LIKE %s 
                           OR stk_articulos_codigos.codigo LIKE %s OR stk_tipos_articulo.nombre LIKE %s)"""
                search = f"%{search_query}%"
                params.extend([search, search, search, search])
            
        sql += " ORDER BY stk_articulos.nombre LIMIT 100"
        
        cursor.execute(sql, params)
        articulos = cursor.fetchall()

        # Inyectar metadatos de balanza
        if found_dynamic and articulos:
            for a in articulos:
                a['dynamic_barcode'] = True
                a['dynamic_value'] = dynamic_value
                a['dynamic_type'] = parsed['tipo']
    
    return jsonify(articulos)

@ventas_bp.route('/ventas/facturar', methods=['GET'])
@login_required
def facturar():
    with get_db_cursor(dictionary=True) as cursor:
        # Clientes
        cursor.execute("SELECT id, nombre, cuit, tipo_responsable FROM erp_terceros WHERE enterprise_id = %s AND es_cliente = 1 AND activo = 1", (g.user['enterprise_id'],))
        clientes = cursor.fetchall()

        # Condición IVA Empresa
        cursor.execute("SELECT condicion_iva FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
        empresa = cursor.fetchone()
        condicion_iva_empresa = empresa['condicion_iva'] if empresa else 'Responsable Inscripto'
        
        # Obtener naturalezas disponibles para el filtro
        try:
            cursor.execute("""
                SELECT DISTINCT stk_tipos_articulo.naturaleza 
                FROM stk_tipos_articulo
                WHERE (stk_tipos_articulo.enterprise_id = 0 OR stk_tipos_articulo.enterprise_id = %s) AND stk_tipos_articulo.naturaleza IS NOT NULL AND stk_tipos_articulo.naturaleza != ''
                ORDER BY stk_tipos_articulo.naturaleza
            """, (g.user['enterprise_id'],))
            naturalezas = [row['naturaleza'] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error cargando naturalezas: {e}")
            naturalezas = []
        
        # Tipos de Comprobante
        allowed_codigos = BillingService.get_allowed_comprobantes(condicion_iva_empresa, '*')
        if not allowed_codigos:
            allowed_codigos = ['006', '007', '008']
        placeholders = ', '.join(['%s'] * len(allowed_codigos))
        cursor.execute(f"SELECT codigo, descripcion, letra FROM sys_tipos_comprobante WHERE codigo IN ({placeholders})", tuple(allowed_codigos))
        tipos = cursor.fetchall()
        
        # Depósitos
        cursor.execute("SELECT id, nombre FROM stk_depositos WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1", (g.user['enterprise_id'],))
        depositos = cursor.fetchall()
 
        # Condiciones de Pago
        cursor.execute("SELECT id, nombre, dias_vencimiento, descuento_pct FROM fin_condiciones_pago WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1 ORDER BY nombre", (g.user['enterprise_id'],))
        condiciones = cursor.fetchall()
 
        # Medios de Pago (Sin retenciones/percepciones para cobro manual)
        cursor.execute("""
            SELECT id, nombre, recargo_pct, tipo 
            FROM fin_medios_pago 
            WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1 
            AND tipo NOT IN ('RETENCION', 'PERCEPCION')
            ORDER BY nombre
        """, (g.user['enterprise_id'],))
        medios_pago = cursor.fetchall()
        
        # Jurisdicciones donde la empresa es agente de percepción
        cursor.execute("""
            SELECT jurisdiccion FROM sys_enterprises_fiscal 
            WHERE enterprise_id = %s AND activo = 1 AND tipo IN ('PERCEPCION', 'AMBOS')
        """, (g.user['enterprise_id'],))
        agente_percepciones = [j['jurisdiccion'].upper() for j in cursor.fetchall()]

        # Transportistas (Logística)
        cursor.execute("SELECT id, nombre, cuit FROM stk_logisticas WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1 ORDER BY nombre", (g.user['enterprise_id'],))
        transportistas = cursor.fetchall()

    return render_template('ventas/facturar.html', 
                           clientes=clientes, 
                           naturalezas=naturalezas, 
                           tipos_comprobante=tipos, 
                           depositos=depositos, 
                           medios_pago=medios_pago, 
                           condiciones=condiciones, 
                           condicion_iva_empresa=condicion_iva_empresa,
                           agente_percepciones=agente_percepciones,
                           transportistas=transportistas,
                           now=datetime.date.today().isoformat())

@ventas_bp.route('/ventas/nota-credito/<int:factura_id>')
@login_required
def nota_credito(factura_id):
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT c.*, t.nombre as tercero_nombre, cp.nombre as condicion_pago_nombre
            FROM erp_comprobantes c
            JOIN erp_terceros t ON c.tercero_id = t.id
            LEFT JOIN fin_condiciones_pago cp ON c.condicion_pago_id = cp.id
            WHERE c.id = %s AND c.enterprise_id = %s
        """, (factura_id, g.user['enterprise_id']))
        factura = cursor.fetchone()

        # Condición IVA Empresa
        cursor.execute("SELECT condicion_iva FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
        empresa = cursor.fetchone()
        condicion_iva_empresa = empresa['condicion_iva'] if empresa else 'Responsable Inscripto'
        
        if not factura:
            flash("Factura no encontrada", "error")
            return redirect(url_for('ventas.comprobantes'))
            
        cursor.execute("""
            SELECT erp_comprobantes_detalle.*, stk_articulos.nombre AS articulo_nombre 
            FROM erp_comprobantes_detalle 
            LEFT JOIN stk_articulos ON erp_comprobantes_detalle.articulo_id = stk_articulos.id 
            WHERE erp_comprobantes_detalle.comprobante_id = %s
        """, (factura_id,))
        items_db = cursor.fetchall()
        
        items_precargados = [{
            'id': i['articulo_id'],
            'nombre': i['articulo_nombre'] or i['descripcion'],
            'cantidad': float(i['cantidad']),
            'precio': float(i['precio_unitario']),
            'iva': float(i['alicuota_iva'])
        } for i in items_db]
        
        cursor.execute("SELECT id, nombre, cuit, tipo_responsable FROM erp_terceros WHERE enterprise_id = %s AND es_cliente = 1 AND activo = 1", (g.user['enterprise_id'],))
        clientes = cursor.fetchall()
        
        mapa_nc = {'001': '003', '006': '008', '011': '013'}
        tipo_nc = mapa_nc.get(factura['tipo_comprobante'], '013')
        
        cursor.execute("SELECT codigo, descripcion FROM sys_tipos_comprobante WHERE codigo IN ('003', '008', '013')")
        tipos = cursor.fetchall()

        # Obtener depósito origen de la factura original
        cursor.execute("""
            SELECT m.deposito_destino_id 
            FROM stk_movimientos m 
            WHERE m.comprobante_id = %s AND m.enterprise_id = %s LIMIT 1
        """, (factura_id, g.user['enterprise_id']))
        mov = cursor.fetchone()
        deposito_sugerido = mov['deposito_destino_id'] if mov else None

        # Obtener pagos originales de la factura
        cursor.execute("""
            SELECT mc.medio_pago_id, mp.nombre as medio_nombre, mc.importe
            FROM fin_factura_cobros mc
            JOIN fin_medios_pago mp ON mc.medio_pago_id = mp.id
            WHERE mc.factura_id = %s AND mc.enterprise_id = %s
        """, (factura_id, g.user['enterprise_id']))
        pagos_originales = cursor.fetchall()

        cursor.execute("SELECT id, nombre FROM stk_depositos WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1", (g.user['enterprise_id'],))
        depositos = cursor.fetchall()
 
        try:
            cursor.execute("""
                SELECT id, nombre, recargo_pct, tipo 
                FROM fin_medios_pago 
                WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1 
                AND tipo NOT IN ('RETENCION', 'PERCEPCION')
                ORDER BY nombre
            """, (g.user['enterprise_id'],))
            medios_pago = cursor.fetchall()
        except Exception:
            medios_pago = []
 
        # Condiciones de Pago
        cursor.execute("SELECT id, nombre, dias_vencimiento, descuento_pct FROM fin_condiciones_pago WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1 ORDER BY nombre", (g.user['enterprise_id'],))
        condiciones = cursor.fetchall()
        
        # Jurisdicciones donde la empresa es agente de percepción
        cursor.execute("""
            SELECT jurisdiccion FROM sys_enterprises_fiscal 
            WHERE enterprise_id = %s AND activo = 1 AND tipo IN ('PERCEPCION', 'AMBOS')
        """, (g.user['enterprise_id'],))
        agente_percepciones = [j['jurisdiccion'].upper() for j in cursor.fetchall()]

        # Transportistas
        cursor.execute("SELECT id, nombre FROM stk_logisticas WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1", (g.user['enterprise_id'],))
        transportistas = cursor.fetchall()

    return render_template('ventas/devolucion_solicitud.html', 
                           es_nota_credito=True,
                           factura=factura,
                           cliente_preseleccionado=factura['tercero_id'],
                           items_precargados=items_precargados,
                           tipo_sugerido=tipo_nc,
                           clientes=clientes, naturalezas=[], 
                           tipos_comprobante=tipos, 
                           depositos=depositos, 
                           transportistas=transportistas,
                           deposito_sugerido=deposito_sugerido,
                           medios_pago=medios_pago, 
                           pagos_originales=pagos_originales,
                           condiciones=condiciones, 
                           condicion_iva_empresa=condicion_iva_empresa,
                           agente_percepciones=agente_percepciones,
                           now=datetime.date.today().isoformat())

@ventas_bp.route('/ventas/procesar-factura', methods=['POST'])
@login_required
@atomic_transaction('ventas', severity=8, impact_category='Financial')
def procesar_factura():

    data = request.json
    print(f"DEBUG: Procesando factura. Data: {data}", flush=True)
    cliente_id = data.get('cliente_id')
    tipo_comp = data.get('tipo_comprobante')
    deposito_id = data.get('deposito_id') or 1 
    direccion_entrega_id = data.get('direccion_entrega_id') or None
    receptor_id = data.get('receptor_contacto_id') or None
    items = data.get('items', [])
    pagos = data.get('pagos', []) # Lista de {medio_id, importe}
    condicion_pago_id = data.get('condicion_pago_id') or None
    
    # Referencia a factura original si es NC
    factura_asociada_id = data.get('factura_asociada_id') or None
    
    if not cliente_id or not items:
        return jsonify({'success': False, 'message': 'Datos incompletos'}), 400

    es_nc = tipo_comp in ['003', '008', '013']
    signo_stock = 1 if es_nc else -1

    # === VALIDACION DE CAMPOS DE ENTREGA (Incidente 666) ===
    # Solo para Facturas (no NC), validamos que existan datos de logística antes de impactar nada.
    if not es_nc:
        transportista = data.get('transportista_nombre')
        patente = data.get('vehiculo_patente') or data.get('patente') # Soporte ambos nombres de campo
        
        if not direccion_entrega_id:
            return jsonify({'success': False, 'message': '🚨 Faltan datos de Entrega: Debe seleccionar un Destino.'}), 400
        if not transportista:
            return jsonify({'success': False, 'message': '🚨 Faltan datos de Logística: Debe seleccionar un Transportista.'}), 400
        if not patente:
            return jsonify({'success': False, 'message': '🚨 Faltan datos de Logística: Debe ingresar la Patente del vehículo.'}), 400

    try:
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Calcular totales
            total_neto = 0
            total_iva = 0
            total_factura = 0
            detalles = []
            
            for item in items:
                calc = BillingService.calculate_item_totals(item['cantidad'], item['precio'], item['iva'])
                total_neto += calc['neto']
                total_iva += calc['iva']
                total_factura += calc['total']
                detalles.append({
                    'articulo_id': item['id'],
                    'descripcion': item['nombre'],
                    'cantidad': item['cantidad'],
                    'precio_unitario': item['precio'],
                    'alicuota_iva': item['iva'],
                    'neto': calc['neto'],
                    'iva': calc['iva'],
                    'total': calc['total']
                })
            
            # --- CALCULO DINAMICO DE PERCEPCIONES IIBB ---
            # Obtener jurisdicciones donde la empresa actúa como agente de percepción
            cursor.execute("""
                SELECT sef.jurisdiccion, sef.id as fiscal_id
                FROM sys_enterprises_fiscal sef
                WHERE sef.enterprise_id = %s AND sef.activo = 1 AND sef.tipo IN ('PERCEPCION', 'AMBOS')
            """, (g.user['enterprise_id'],))
            juris_agente = {j['jurisdiccion'].upper(): j['fiscal_id'] for j in cursor.fetchall()}

            # Obtener catálogo de impuestos para IIBB
            cursor.execute("""
                SELECT id FROM sys_impuestos WHERE nombre LIKE '%IIBB%' OR nombre LIKE '%Ingresos Brutos%' LIMIT 1
            """)
            imp_iibb_row = cursor.fetchone()
            impuesto_iibb_id = imp_iibb_row['id'] if imp_iibb_row else None

            percepciones_detalle = []
            if juris_agente:
                cursor.execute("""
                    SELECT jurisdiccion, alicuota FROM erp_datos_fiscales
                    WHERE tercero_id = %s AND enterprise_id = %s
                """, (cliente_id, g.user['enterprise_id']))
                fiscal_cliente = cursor.fetchall()

                for df in fiscal_cliente:
                    juris_upper = df['jurisdiccion'].upper()
                    if juris_upper in juris_agente:
                        alic = Decimal(str(df['alicuota']))
                        if alic > 0:
                            imp = (Decimal(str(total_neto)) * alic / Decimal('100')).quantize(Decimal('0.01'))
                            percepciones_detalle.append({
                                'jurisdiccion': df['jurisdiccion'],
                                'alicuota': alic,
                                'importe': imp,
                                'impuesto_id': impuesto_iibb_id,
                            })

            total_perc = sum([p['importe'] for p in percepciones_detalle])
            total_factura += total_perc

            # --- Calcular fecha de vencimiento según condición de pago ---
            fecha_vencimiento = None
            if condicion_pago_id:
                cursor.execute("SELECT dias_vencimiento FROM fin_condiciones_pago WHERE id = %s", (condicion_pago_id,))
                cond_row = cursor.fetchone()
                if cond_row and cond_row['dias_vencimiento'] is not None:
                    import datetime as dt
                    fecha_vencimiento = (dt.date.today() + dt.timedelta(days=int(cond_row['dias_vencimiento']))).isoformat()

            # Detectar estado de pago inicial
            total_pagado = sum([float(p['importe']) for p in pagos])
            estado_pago = 'PAGADO' if total_pagado >= float(total_factura) - 0.01 else ('PARCIAL' if total_pagado > 0 else 'PENDIENTE')

            # 2. Guardar Cabecera
            # Obtener CUIT de la empresa y del cliente
            cursor.execute("SELECT cuit FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
            ent_row = cursor.fetchone()
            ent_cuit = ent_row['cuit'] if ent_row else ''

            cursor.execute("SELECT cuit FROM erp_terceros WHERE id = %s", (cliente_id,))
            ter_row = cursor.fetchone()
            cliente_cuit = ter_row['cuit'] if ter_row else ''
            from services.numeration_service import NumerationService
            proximo_nro = NumerationService.get_next_number(g.user['enterprise_id'], 'COMPROBANTE', tipo_comp, 1)
            
            # Map legacy columns
            p_arba = sum([p['importe'] for p in percepciones_detalle if 'ARBA' in p['jurisdiccion'].upper()])
            p_agip = sum([p['importe'] for p in percepciones_detalle if 'AGIP' in p['jurisdiccion'].upper() or 'CABA' in p['jurisdiccion'].upper()])

            cursor.execute("""
                INSERT INTO erp_comprobantes (
                    enterprise_id, modulo, tipo_operacion, emisor_cuit, receptor_cuit,
                    tercero_id, tipo_comprobante, punto_venta, numero, cot, fecha_emision,
                    fecha_vencimiento,
                    importe_neto, importe_iva, importe_total,
                    importe_percepcion_iibb_arba, importe_percepcion_iibb_agip,
                    estado_pago, direccion_entrega_id, receptor_contacto_id, condicion_pago_id,
                    transportista_nombre, transportista_cuit, vehiculo_patente,
                    comprobante_asociado_id, user_id
                )
                VALUES (%s, 'VENTAS', 'VENTA', %s, %s, %s, %s, 1, %s, %s, CURRENT_DATE,
                    %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                g.user['enterprise_id'], ent_cuit, cliente_cuit,
                cliente_id, tipo_comp, proximo_nro, data.get('cot'),
                fecha_vencimiento,
                total_neto, total_iva, total_factura,
                p_arba, p_agip,
                estado_pago, direccion_entrega_id, receptor_id, condicion_pago_id,
                data.get('transportista_nombre'), data.get('transportista_cuit'), data.get('vehiculo_patente'),
                factura_asociada_id, g.user['id']
            ))
            
            comp_id = cursor.lastrowid

            # Actualizar último número en parámetros
            NumerationService.update_last_number(g.user['enterprise_id'], 'COMPROBANTE', tipo_comp, 1, proximo_nro)

            # 2.5 Guardar Detalles de Percepciones Dinámicas (con trazabilidad completa)
            for p in percepciones_detalle:
                cursor.execute("""
                    INSERT INTO erp_comprobantes_impuestos
                        (enterprise_id, comprobante_id, impuesto_id, jurisdiccion, alicuota, base_imponible, importe, user_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (
                    g.user['enterprise_id'], comp_id, p.get('impuesto_id'),
                    p['jurisdiccion'], p['alicuota'], total_neto, p['importe'],
                    g.user['id']
                ))
            
            # 3. Registrar Pagos con snapshot de cuenta contable
            if pagos:
                for p in pagos:
                    # Obtener cuenta_contable_id actual del medio de pago (snapshot inmutable)
                    cursor.execute(
                        "SELECT cuenta_contable_id FROM fin_medios_pago WHERE id = %s AND enterprise_id IN (0, %s)",
                        (p['medio_id'], g.user['enterprise_id'])
                    )
                    mp_row = cursor.fetchone()
                    cuenta_snapshot_id = mp_row['cuenta_contable_id'] if mp_row else None

                    cursor.execute("""
                        INSERT INTO fin_factura_cobros
                            (enterprise_id, factura_id, medio_pago_id, cuenta_contable_snapshot_id, importe, fecha, user_id)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                    """, (
                        g.user['enterprise_id'], comp_id, p['medio_id'],
                        cuenta_snapshot_id, p['importe'], g.user['id']
                    ))

            # 4. Movimiento Stock
            # En ventas: origen = depósito de la empresa, destino = NULL (calle/cliente)
            # En NC/devolución: origen = NULL (viene del cliente), destino = depósito de la empresa
            dep_origen = None if es_nc else deposito_id
            dep_destino = deposito_id if es_nc else None

            cursor.execute("""
                INSERT INTO stk_movimientos
                    (enterprise_id, motivo_id, deposito_origen_id, deposito_destino_id, comprobante_id, tercero_id, user_id, observaciones, fecha)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (
                g.user['enterprise_id'], 33 if es_nc else 31,
                dep_origen, dep_destino,
                comp_id, cliente_id, g.user['id'],
                'Devolución (NC)' if es_nc else 'Venta Factura Automática'
            ))
            mov_id = cursor.lastrowid
            
            # 5. Detalles Factura y Stock
            for d in detalles:
                cursor.execute("""
                    INSERT INTO erp_comprobantes_detalle (enterprise_id, comprobante_id, articulo_id, descripcion, cantidad, precio_unitario, alicuota_iva, subtotal_neto, importe_iva, subtotal_total, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (g.user['enterprise_id'], comp_id, d['articulo_id'], d['descripcion'], d['cantidad'], d['precio_unitario'], d['alicuota_iva'], d['neto'], d['iva'], d['total'], g.user['id']))
                
                if d['articulo_id']:
                    cursor.execute("""
                        INSERT INTO stk_movimientos_detalle (movimiento_id, articulo_id, cantidad, user_id)
                        VALUES (%s, %s, %s, %s)
                    """, (mov_id, d['articulo_id'], d['cantidad'], g.user['id']))
                    
                    cantidad_delta = d['cantidad'] * signo_stock
                    cursor.execute("""
                        INSERT INTO stk_existencias (enterprise_id, deposito_id, articulo_id, cantidad, user_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE cantidad = cantidad + %s, last_updated = CURRENT_TIMESTAMP, user_id_update = %s
                    """, (g.user['enterprise_id'], deposito_id, d['articulo_id'], cantidad_delta, g.user['id'], cantidad_delta, g.user['id']))

            # 6. Generar Asiento Contable Automático
            asiento_id = _generar_asiento_contable(cursor, comp_id, g.user['enterprise_id'], es_nc, g.user['id'])
            if asiento_id:
                cursor.execute("UPDATE erp_comprobantes SET asiento_id = %s WHERE id = %s AND enterprise_id = %s", (asiento_id, comp_id, g.user['enterprise_id']))

            # 7. Solicitar CAE a AFIP (Modo Transaccional)
            import asyncio as _asyncio
            from services.afip_service import AfipService
            
            # Pasamos el cursor actual para que AFIP trabaje en la misma transaccion
            afip_res = _asyncio.run(AfipService.solicitar_cae(g.user['enterprise_id'], comp_id, cursor=cursor))
            
            if not afip_res.get('success'):
                # SI AFIP RECHAZA: Lanzamos excepción para que @atomic_transaction haga ROLLBACK de todo
                error_msg = afip_res.get('error', 'Error desconocido en AFIP')
                raise Exception(f"AFIP Rechazó la Operación: {error_msg}")
            
            # El total_perc ya fue calculado arriba desde la BD
            if total_perc > 0:
                try:
                    # Obtener Data Completa para el PDF (similar a ver_comprobante)
                    cursor.execute("""
                        SELECT c.*, t.nombre as cliente_nombre, t.cuit as cliente_cuit, t.tipo_responsable as cliente_condicion, t.email as cliente_email,
                               tc.descripcion as tipo_nombre, tc.letra,
                               d.etiqueta as entrega_etiqueta, d.calle as entrega_calle, d.numero as entrega_numero, 
                               d.localidad as entrega_localidad, d.provincia as entrega_provincia,
                               con.nombre as receptor_nombre,
                               casoc.punto_venta as asoc_punto_venta, casoc.numero as asoc_numero
                        FROM erp_comprobantes c
                        LEFT JOIN erp_terceros t ON c.tercero_id = t.id
                        LEFT JOIN sys_tipos_comprobante tc ON c.tipo_comprobante = tc.codigo
                        LEFT JOIN erp_direcciones d ON c.direccion_entrega_id = d.id
                        LEFT JOIN erp_contactos con ON c.receptor_contacto_id = con.id
                        LEFT JOIN erp_comprobantes casoc ON c.comprobante_asociado_id = casoc.id
                        WHERE c.id = %s AND c.enterprise_id = %s
                    """, (comp_id, g.user['enterprise_id']))
                    comprobante = cursor.fetchone()

                    if comprobante and comprobante['cliente_email']:
                        cursor.execute("SELECT * FROM erp_comprobantes_detalle WHERE comprobante_id = %s AND enterprise_id = %s", (comp_id, g.user['enterprise_id']))
                        detalles_pdf = cursor.fetchall()
                        
                        cursor.execute("SELECT * FROM erp_direcciones WHERE tercero_id = %s AND enterprise_id = %s AND es_fiscal = 1 LIMIT 1", (comprobante['tercero_id'], g.user['enterprise_id']))
                        direccion_pdf = cursor.fetchone()

                        cursor.execute("SELECT * FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
                        empresa_pdf = cursor.fetchone()

                        layout = BillingService.get_layout(g.user['enterprise_id'])
                        # Pasamos percepciones_detalle como impuestos
                        vals = BillingService.prepare_invoice_values(comprobante, detalles_pdf, empresa_pdf, direccion_pdf, percepciones_detalle)
                        html_pdf = render_template('ventas/comprobante_impresion.html', 
                                               c=comprobante, detalles=detalles_pdf, cliente_dir=direccion_pdf, 
                                               empresa=empresa_pdf, layout=layout, vals=vals, impuestos=percepciones_detalle)
                        
                        # Generar PDF
                        pdf_out = io.BytesIO()
                        pisa.CreatePDF(io.StringIO(html_pdf), dest=pdf_out)
                        pdf_content = pdf_out.getvalue()

                        # Enviar Email
                        fact_nro = f"{comprobante['punto_venta']:05d}-{comprobante['numero']:08d}"
                        subject, html_body = enviar_notificacion_percepcion(
                            comprobante['cliente_email'], comprobante['cliente_nombre'], fact_nro, total_perc, g.user['enterprise_id']
                        )

                        pdf_filename = f"Factura_{fact_nro}.pdf"
                        _enviar_email(comprobante['cliente_email'], subject, html_body, [(pdf_filename, pdf_content)], enterprise_id=g.user['enterprise_id'])

                except Exception as e_mail:
                    print(f"Error al enviar mail de percepcion: {e_mail}")

            return jsonify({'success': True, 'message': 'Factura generada con éxito', 'id': comp_id})
            
    except Exception as e:
        print(f"DEBUG ERROR en procesar_factura: {str(e)}", flush=True)
        return jsonify({'success': False, 'message': str(e)}), 500

@ventas_bp.route('/ventas/comprobantes')
@login_required
def comprobantes():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT c.*, t.nombre as cliente_nombre, tc.letra, tc.descripcion as tipo_nombre,
                   (SELECT COUNT(*) FROM stk_devoluciones_solicitudes s 
                    WHERE s.comprobante_origen_id = c.id AND s.estado != 'ANULADO') as nc_solicitada,
                   (SELECT COUNT(*) FROM erp_comprobantes nc 
                    WHERE nc.referencia_comercial = CONCAT(LPAD(c.punto_venta, 4, '0'), '-', LPAD(c.numero, 8, '0'))
                    AND nc.tipo_comprobante IN ('003', '008', '013')) as nc_emitida
            FROM erp_comprobantes c
            JOIN erp_terceros t ON c.tercero_id = t.id
            JOIN sys_tipos_comprobante tc ON c.tipo_comprobante = tc.codigo
            WHERE c.enterprise_id = %s 
              AND c.tipo_operacion = 'VENTA'
            ORDER BY c.fecha_emision DESC, c.numero DESC
        """, (g.user['enterprise_id'],))
        lista = cursor.fetchall()
    return render_template('ventas/comprobantes.html', comprobantes=lista)

@ventas_bp.route('/ventas/comprobante/ver/<int:id>')
@login_required
def ver_comprobante(id):
    print(f"DEBUG: Consultando comprobante ID {id} para la empresa {g.user['enterprise_id']}", flush=True)
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Cabecera + Cliente + Tipo (Usamos LEFT JOIN para diagnosticar)
        cursor.execute("""
            SELECT c.*, t.nombre as cliente_nombre, t.cuit as cliente_cuit, t.tipo_responsable as cliente_condicion,
                   tc.descripcion as tipo_nombre, tc.letra,
                   d.etiqueta as entrega_etiqueta, d.calle as entrega_calle, d.numero as entrega_numero, 
                   d.localidad as entrega_localidad, d.provincia as entrega_provincia,
                   con.nombre as receptor_nombre,
                   casoc.punto_venta as asoc_punto_venta, casoc.numero as asoc_numero
            FROM erp_comprobantes c
            LEFT JOIN erp_terceros t ON c.tercero_id = t.id
            LEFT JOIN sys_tipos_comprobante tc ON c.tipo_comprobante = tc.codigo
            LEFT JOIN erp_direcciones d ON c.direccion_entrega_id = d.id
            LEFT JOIN erp_contactos con ON c.receptor_contacto_id = con.id
            LEFT JOIN erp_comprobantes casoc ON c.comprobante_asociado_id = casoc.id
            WHERE c.id = %s AND c.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        comprobante = cursor.fetchone()
        
        if not comprobante:
            print(f"DEBUG: Comprobante {id} NOT FOUND. Empresa en sesión: {g.user['enterprise_id']}", flush=True)
            # Verificación extra: ¿Existe el comprobante pero para OTRA empresa?
            cursor.execute("SELECT enterprise_id FROM erp_comprobantes WHERE id = %s", (id,))
            extra = cursor.fetchone()
            if extra:
                print(f"DEBUG CRÍTICO: El comprobante {id} EXISTE pero pertenece a empresa {extra['enterprise_id']} (Sesión: {g.user['enterprise_id']})", flush=True)
            
            flash("Comprobante no encontrado.", "danger")
            return redirect(url_for('ventas.comprobantes'))
        
        print(f"DEBUG: Comprobante {id} encontrado. Cliente: {comprobante['cliente_nombre']}, Tipo: {comprobante['tipo_nombre']}", flush=True)

        # 2. Detalles
        cursor.execute("""
            SELECT * FROM erp_comprobantes_detalle WHERE comprobante_id = %s AND enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        detalles = cursor.fetchall()
        
        # 3. Dirección del Cliente (Fiscal)
        cursor.execute("SELECT * FROM erp_direcciones WHERE tercero_id = %s AND enterprise_id = %s AND es_fiscal = 1 LIMIT 1", (comprobante['tercero_id'], g.user['enterprise_id']))
        direccion = cursor.fetchone()

        # 3.5 Impuestos / Percepciones Dinámicas
        cursor.execute("SELECT * FROM erp_comprobantes_impuestos WHERE comprobante_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
        impuestos = cursor.fetchall()

        # 4. Datos de la Empresa (Emisor) - Placeholder por ahora si no hay tabla
        cursor.execute("SELECT * FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
        empresa = cursor.fetchone()


        # 5. Obtener Layout y Valores para el Comprobante
        layout = BillingService.get_layout(g.user['enterprise_id'])
        vals = BillingService.prepare_invoice_values(comprobante, detalles, empresa, direccion, impuestos)

        # 6. Impresora Predeterminada para impresión directa (QZ Tray)
        cursor.execute("""
            SELECT * FROM stk_impresoras_config 
            WHERE enterprise_id = %s AND es_predeterminada = 1 AND activo = 1 
            LIMIT 1
        """, (g.user['enterprise_id'],))
        printer = cursor.fetchone()

    # Revisar si se solicito reimpresion usando query string argument ?es_copia=1
    es_copia = request.args.get('es_copia', '0') == '1'

    if es_copia:
        try:
            with get_db_cursor() as cur:
                cur.execute("""
                    INSERT INTO fin_comprobantes_copias 
                    (enterprise_id, comprobante_id, user_id, fecha)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                """, (g.user['enterprise_id'], id, g.user['id']))
        except Exception as e:
            print(f"Error al registrar log de reimpresion: {e}")

    return render_template('ventas/comprobante_impresion.html', 
                           c=comprobante, 
                           detalles=detalles, 
                           cliente_dir=direccion,
                           impuestos=impuestos,
                           empresa=empresa,
                           layout=layout,
                           vals=vals,
                           printer=printer,
                           es_copia=es_copia)

@ventas_bp.route('/ventas/remito/ver/<int:id>')
@login_required
def ver_remito(id):
    """
    Versión para impresión de Remito de un comprobante (sea factura o remito nativo).
    Fuerza el layout de Remito.
    """
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT erp_comprobantes.*, erp_terceros.nombre as cliente_nombre, erp_terceros.cuit as cliente_cuit, erp_terceros.tipo_responsable as cliente_condicion,
                   sys_tipos_comprobante.descripcion as tipo_nombre, sys_tipos_comprobante.letra,
                   erp_direcciones.etiqueta as entrega_etiqueta, erp_direcciones.calle as entrega_calle, erp_direcciones.numero as entrega_numero, 
                   erp_direcciones.localidad as entrega_localidad, erp_direcciones.provincia as entrega_provincia,
                   erp_contactos.nombre as receptor_nombre
            FROM erp_comprobantes
            LEFT JOIN erp_terceros ON erp_comprobantes.tercero_id = erp_terceros.id
            LEFT JOIN sys_tipos_comprobante ON erp_comprobantes.tipo_comprobante = sys_tipos_comprobante.codigo
            LEFT JOIN erp_direcciones ON erp_comprobantes.direccion_entrega_id = erp_direcciones.id
            LEFT JOIN erp_contactos ON erp_comprobantes.receptor_contacto_id = erp_contactos.id
            WHERE erp_comprobantes.id = %s AND erp_comprobantes.enterprise_id = %s
        """, (id, g.user['enterprise_id']))
        comprobante = cursor.fetchone()
        
        if not comprobante:
            flash("Comprobante no encontrado.", "danger")
            return redirect(url_for('ventas.comprobantes'))
        
        # Clonamos y forzamos tipo remito para el BillingService
        c_remito = dict(comprobante)
        if not comprobante['tipo_comprobante'] in ['091', '099']:
            c_remito['tipo_comprobante'] = 'REMITO' 

        cursor.execute("SELECT * FROM erp_comprobantes_detalle WHERE comprobante_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
        detalles = cursor.fetchall()
        
        cursor.execute("SELECT * FROM erp_direcciones WHERE tercero_id = %s AND enterprise_id = %s AND es_fiscal = 1 LIMIT 1", (comprobante['tercero_id'], g.user['enterprise_id']))
        direccion = cursor.fetchone()

        cursor.execute("SELECT * FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
        empresa = cursor.fetchone()

        layout = BillingService.get_layout(g.user['enterprise_id'])
        # Pasamos impuestos vacíos para Remito (usualmente no se muestran desglosados)
        vals = BillingService.prepare_invoice_values(c_remito, detalles, empresa, direccion, [])

        # 6. Impresora Predeterminada para impresión directa (QZ Tray)
        cursor.execute("""
            SELECT * FROM stk_impresoras_config 
            WHERE enterprise_id = %s AND es_predeterminada = 1 AND activo = 1 
            LIMIT 1
        """, (g.user['enterprise_id'],))
        printer = cursor.fetchone()

    return render_template('ventas/comprobante_impresion.html', 
                           c=c_remito, 
                           detalles=detalles, 
                           cliente_dir=direccion, 
                           empresa=empresa, 
                           layout=layout, 
                           vals=vals,
                           printer=printer)

@ventas_bp.route('/ventas/devolucion-solicitar', methods=['POST'])
@login_required
def api_devolucion_solicitar():
    data = request.json
    enterprise_id = g.user['enterprise_id']
    user_id = g.user['id']
    
    factura_id = data.get('factura_id')
    cliente_id = data.get('cliente_id')
    deposito_id = data.get('deposito_id')
    logistica_id_raw = data.get('logistica_id')
    
    # Manejo seguro de IDs
    try:
        factura_id = int(factura_id) if factura_id else None
        cliente_id = int(cliente_id) if cliente_id else None
        deposito_id = int(deposito_id) if deposito_id else None
        logistica_id = int(logistica_id_raw) if logistica_id_raw and str(logistica_id_raw).strip() != "" else None
    except (ValueError, TypeError):
        return jsonify({'success': False, 'message': 'IDs de referencia inválidos'}), 400
    observaciones = data.get('observaciones', '')
    items = data.get('items', [])
    reembolsos = data.get('reembolsos', [])
    
    if not factura_id or not items:
        return jsonify({'success': False, 'message': 'Datos incompletos'}), 400
        
    try:
        with get_db_cursor() as cursor:
            # Si no se eligió transportista, usar "Consumidor Final / Propia"
            if not logistica_id:
                cursor.execute(
                    "SELECT id FROM stk_logisticas WHERE nombre = %s AND activo = 1 LIMIT 1",
                    ('Consumidor Final / Propia',)
                )
                row = cursor.fetchone()
                logistica_id = row[0] if row else None

            # 1. Guardar Cabecera de Solicitud
            cursor.execute("""
                INSERT INTO stk_devoluciones_solicitudes (
                    enterprise_id, tercero_id, comprobante_origen_id, 
                    deposito_destino_id, logistica_id,
                    observaciones, user_id_solicita, estado, fecha_solicitud
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDIENTE', CURRENT_TIMESTAMP)
            """, (
                enterprise_id, cliente_id, factura_id,
                deposito_id, logistica_id,
                observaciones, user_id
            ))
            solicitud_id = cursor.lastrowid
            
            # 2. Guardar Detalles
            for item in items:
                cursor.execute("""
                    INSERT INTO stk_devoluciones_solicitudes_det (
                        solicitud_id, articulo_id, cantidad_solicitada, 
                        precio_unitario, alicuota_iva, user_id
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    solicitud_id, item['articulo_id'], item['cantidad'],
                    item['precio'], item['iva'], g.user['id']
                ))
            
            # 3. Guardar Reembolsos Sugeridos
            for r in reembolsos:
                cursor.execute("""
                    INSERT INTO fin_devoluciones_valores (
                        solicitud_id, medio_pago_id, importe, user_id
                    ) VALUES (%s, %s, %s, %s)
                """, (solicitud_id, r['medio_id'], r['importe'], g.user['id']))
            
            # --- NOTIFICACION POR EMAIL ---
            # 1. Obtener email del cliente
            cursor.execute("SELECT email, nombre FROM erp_terceros WHERE id = %s", (cliente_id,))
            cliente = cursor.fetchone()
            
            # 2. Obtener número de factura origen
            cursor.execute("SELECT punto_venta, numero FROM erp_comprobantes WHERE id = %s", (factura_id,))
            fac = cursor.fetchone()
            factura_nro = f"{fac[0]:04d}-{fac[1]:08d}" if fac else "Desconocida"
            
            # 3. Preparar items con nombres para el mail
            items_enriquecidos = []
            for item in items:
                cursor.execute("SELECT nombre FROM stk_articulos WHERE id = %s", (item['articulo_id'],))
                art = cursor.fetchone()
                items_enriquecidos.append({
                    'nombre': art[0] if art else f"Art.#{item['articulo_id']}",
                    'cantidad': item['cantidad']
                })
            
            if cliente and cliente[0]:
                try:
                    enviar_solicitud_devolucion(
                        cliente[0], cliente[1], solicitud_id, 
                        factura_nro, items_enriquecidos, enterprise_id
                    )
                except Exception as ex:
                    print(f"Error enviando mail de devolucion: {str(ex)}")
            
        return jsonify({'success': True, 'message': 'Solicitud generada y notificada por email', 'id': solicitud_id})
        
    except Exception as e:
        print(f"Error en api_devolucion_solicitar: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@ventas_bp.route('/ventas/logistica/devoluciones')
@login_required
def logistica_devoluciones():
    enterprise_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT stk_devoluciones_solicitudes.*, erp_terceros.nombre as cliente_nombre, erp_comprobantes.numero as factura_nro, erp_comprobantes.punto_venta as factura_pv
            FROM stk_devoluciones_solicitudes
            JOIN erp_terceros ON stk_devoluciones_solicitudes.tercero_id = erp_terceros.id
            JOIN erp_comprobantes ON stk_devoluciones_solicitudes.comprobante_origen_id = erp_comprobantes.id
            WHERE stk_devoluciones_solicitudes.enterprise_id = %s AND stk_devoluciones_solicitudes.estado NOT IN ('PROCESADO', 'CANCELADO')
            ORDER BY stk_devoluciones_solicitudes.fecha_solicitud DESC
        """, (enterprise_id,))
        solicitudes = cursor.fetchall()
    return render_template('ventas/logistica_devoluciones.html', solicitudes=solicitudes)

@ventas_bp.route('/ventas/logistica/devolucion/<int:id>')
@login_required
def logistica_ver_devolucion(id):
    enterprise_id = g.user['enterprise_id']
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT stk_devoluciones_solicitudes.*, erp_terceros.nombre as cliente_nombre, erp_comprobantes.numero as factura_nro, erp_comprobantes.punto_venta as factura_pv
            FROM stk_devoluciones_solicitudes
            JOIN erp_terceros ON stk_devoluciones_solicitudes.tercero_id = erp_terceros.id
            JOIN erp_comprobantes ON stk_devoluciones_solicitudes.comprobante_origen_id = erp_comprobantes.id
            WHERE stk_devoluciones_solicitudes.id = %s AND stk_devoluciones_solicitudes.enterprise_id = %s
        """, (id, enterprise_id))
        solicitud = cursor.fetchone()
        
        if not solicitud:
            flash("Solicitud no encontrada", "error")
            return redirect(url_for('ventas.logistica_devoluciones'))
            
        cursor.execute("""
            SELECT stk_devoluciones_solicitudes_det.*, stk_articulos.nombre AS articulo_nombre
            FROM stk_devoluciones_solicitudes_det
            JOIN stk_articulos ON stk_devoluciones_solicitudes_det.articulo_id = stk_articulos.id
            WHERE stk_devoluciones_solicitudes_det.solicitud_id = %s
        """, (id,))
        detalles = cursor.fetchall()

        cursor.execute("SELECT id, nombre, cuit FROM stk_logisticas WHERE (enterprise_id = 0 OR enterprise_id = %s) AND activo = 1", (enterprise_id,))
        transportistas = cursor.fetchall()
        
    return render_template('ventas/logistica_recepcion.html', solicitud=solicitud, detalles=detalles, transportistas=transportistas)

@ventas_bp.route('/ventas/logistica/confirmar-recepcion', methods=['POST'])
@login_required
@atomic_transaction('ventas', severity=8, impact_category='Financial')
def api_confirmar_recepcion():
    data = request.json
    solicitud_id = data.get('solicitud_id')
    recepciones = data.get('recepciones', []) # {detalle_id, cantidad_recibida}
    
    if not solicitud_id or not recepciones:
        return jsonify({'success': False, 'message': 'Datos incompletos'}), 400
        
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Gestionar Transportista (si es nuevo)
            logistica_id = data.get('logistica_id')
            if not logistica_id and data.get('nuevo_transp_nombre'):
                cursor.execute("""
                    INSERT INTO stk_logisticas (enterprise_id, nombre, cuit, activo)
                    VALUES (%s, %s, %s, 1)
                """, (g.user['enterprise_id'], data.get('nuevo_transp_nombre'), data.get('nuevo_transp_cuit')))
                logistica_id = cursor.lastrowid

            # 2. Actualizar cantidades recibidas y datos de entrega en la solicitud
            for r in recepciones:
                cursor.execute("""
                    UPDATE stk_devoluciones_solicitudes_det 
                    SET cantidad_recibida = %s 
                    WHERE id = %s AND solicitud_id = %s
                """, (r['cantidad'], r['detalle_id'], solicitud_id))

            cursor.execute("""
                UPDATE stk_devoluciones_solicitudes 
                SET estado = 'PROCESADO', 
                    user_id_logistica = %s,
                    logistica_id = %s,
                    entrega_persona_nombre = %s,
                    entrega_persona_doc_tipo = %s,
                    entrega_persona_doc_nro = %s
                WHERE id = %s
            """, (
                g.user['id'], 
                logistica_id,
                data.get('entrega_persona_nombre'),
                data.get('entrega_persona_doc_tipo'),
                data.get('entrega_persona_doc_nro'),
                solicitud_id
            ))
            
            # --- 3. DISPARAR PROCESO DE NOTA DE CREDITO ---
            res_nc = _ejecutar_procesamiento_nc_final(cursor, solicitud_id)
            
            if not res_nc['success']:
                raise Exception(res_nc['message'])
                
        return jsonify({'success': True, 'message': 'Recepción confirmada y Nota de Crédito generada', 'id_nc': res_nc['id_nc']})
        
    except Exception as e:
        print(f"Error en confirmar_recepcion: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

def _ejecutar_procesamiento_nc_final(cursor, solicitud_id):
    """
    Realiza las operaciones de:
    - Generación de Nota de Crédito (erp_comprobantes)
    - Devolución de valores (fin_recibos o fin_movimientos)
    - Movimientos de stock (stk_movimientos)
    - Asiento contable (reversión o nuevo crédito fiscal)
    """
    enterprise_id = g.user['enterprise_id']
    
    # Obtener datos de la solicitud
    cursor.execute("""
        SELECT stk_devoluciones_solicitudes.*, erp_comprobantes.tipo_comprobante as tipo_origen, erp_comprobantes.fecha_emision as fecha_origen
        FROM stk_devoluciones_solicitudes
        JOIN erp_comprobantes ON stk_devoluciones_solicitudes.comprobante_origen_id = erp_comprobantes.id
        WHERE stk_devoluciones_solicitudes.id = %s
    """, (solicitud_id,))
    sol = cursor.fetchone()
    
    # Obtener detalles recibidos
    cursor.execute("SELECT * FROM stk_devoluciones_solicitudes_det WHERE solicitud_id = %s AND cantidad_recibida > 0", (solicitud_id,))
    detalles = cursor.fetchall()
    
    # A. Calcular Totales de lo RECIBIDO
    neto_nc = 0
    iva_nc = 0
    for d in detalles:
        neto_nc += float(d['cantidad_recibida']) * float(d['precio_unitario'])
        iva_nc += float(d['cantidad_recibida']) * float(d['precio_unitario']) * (float(d['alicuota_iva']) / 100)
    total_nc = neto_nc + iva_nc
    
    # B. Determinar Tipo NC
    mapa_nc = {'001': '003', '006': '008', '011': '013'}
    tipo_nc = mapa_nc.get(sol['tipo_origen'], '013')
    
    # Obtener próximo número
    from services.numeration_service import NumerationService
    proximo_nro = NumerationService.get_next_number(enterprise_id, 'COMPROBANTE', tipo_nc, 1)
    
    # C. Obtener Datos de Logística si existen
    transp_nombre = None
    transp_cuit = None
    if sol['logistica_id']:
        cursor.execute("SELECT nombre, cuit FROM stk_logisticas WHERE id = %s", (sol['logistica_id'],))
        lg = cursor.fetchone()
        if lg:
            transp_nombre = lg['nombre']
            transp_cuit = lg['cuit']

    # D. Generar Comprobante NC
    cursor.execute("""
        INSERT INTO erp_comprobantes (
            enterprise_id, modulo, tercero_id, tipo_comprobante, punto_venta, numero, 
            fecha_emision, importe_neto, importe_iva, importe_total, estado_pago,
            condicion_pago_id, logistica_id, transportista_nombre, transportista_cuit,
            comprobante_asociado_id
        ) VALUES (%s, 'VENTAS', %s, %s, 1, %s, CURRENT_DATE, %s, %s, %s, 'PAGADO', %s, %s, %s, %s, %s)
    """, (
        enterprise_id, sol['tercero_id'], tipo_nc, proximo_nro,
        neto_nc, iva_nc, total_nc, sol['condicion_devolucion_id'],
        sol['logistica_id'], transp_nombre, transp_cuit,
        sol['comprobante_origen_id']
    ))
    id_nc = cursor.lastrowid
    
    # Actualizar tabla de numeración
    NumerationService.update_last_number(enterprise_id, 'COMPROBANTE', tipo_nc, 1, proximo_nro)
    
    # D. Detalles de NC
    for d in detalles:
        cursor.execute("""
            INSERT INTO erp_comprobantes_detalle (
                enterprise_id, comprobante_id, articulo_id, descripcion, 
                cantidad, precio_unitario, alicuota_iva, neto, iva, total
            )
            SELECT %s, %s, %s, nombre, %s, %s, %s, %s, %s, %s
            FROM stk_articulos WHERE id = %s
        """, (
            enterprise_id, id_nc, d['articulo_id'], d['cantidad_recibida'],
            d['precio_unitario'], d['alicuota_iva'],
            float(d['cantidad_recibida']) * float(d['precio_unitario']),
            float(d['cantidad_recibida']) * float(d['precio_unitario']) * (float(d['alicuota_iva']) / 100),
            float(d['cantidad_recibida']) * float(d['precio_unitario']) * (1 + float(d['alicuota_iva']) / 100),
            d['articulo_id']
        ))
    
    # E. Movimiento de Stock (Ingreso al depósito de recepción)
    cursor.execute("""
        INSERT INTO stk_movimientos
            (enterprise_id, motivo_id, deposito_destino_id, comprobante_id, tercero_id, user_id, observaciones, fecha)
        VALUES (%s, 33, %s, %s, %s, %s, 'Devolución confirmada por Logística (NC Autogestionada)', CURRENT_TIMESTAMP)
    """, (
        enterprise_id, sol['deposito_destino_id'], id_nc, sol['tercero_id'], g.user['id']
    ))
    mov_id = cursor.lastrowid
    
    for d in detalles:
        cursor.execute("""
            INSERT INTO stk_movimientos_detalle (movimiento_id, articulo_id, cantidad, signo)
            VALUES (%s, %s, %s, 1)
        """, (mov_id, d['articulo_id'], d['cantidad_recibida']))
        
    # F. Reembolsos (Registro en Caja si corresponde)
    # Por ahora solo marcamos que hubo reembolsos sugeridos, en una fase futura
    # se integrará con fin_recibos_egresos.
    
    # G. Lógica de IVA y Asiento Contable (Diferencia de mes)
    import datetime
    fecha_hoy = datetime.date.today()
    fecha_origen = sol['fecha_origen']
    es_mismo_mes = (fecha_hoy.month == fecha_origen.month and fecha_hoy.year == fecha_origen.year)
    
    asiento_id = _generar_asiento_nc_avanzado(cursor, id_nc, sol['comprobante_origen_id'], es_mismo_mes)
    
    # Vincular asiento al comprobante
    if asiento_id:
        cursor.execute("UPDATE erp_comprobantes SET asiento_id = %s WHERE id = %s", (asiento_id, id_nc))
        
    # --- LLAMADO A AFIP PARA LA NC (Transaccional) ---
    import asyncio as _asyncio
    from services.afip_service import AfipService
    afip_res = _asyncio.run(AfipService.solicitar_cae(enterprise_id, id_nc, cursor=cursor))
    if not afip_res.get('success'):
        raise Exception(f"AFIP Rechazó la Nota de Crédito: {afip_res.get('error')}")
    
    return {'success': True, 'id_nc': id_nc}

def _generar_asiento_nc_avanzado(cursor, id_nc, id_factura, es_mismo_mes):
    """
    Lógica contable avanzada para NC:
    - Mismo mes: Revierte IVA Débito Fiscal (2.2.01).
    - Mes diferente: Usa IVA Crédito Fiscal (2.1.01) si existe, o mantiene Débito pero con marca.
    """
    enterprise_id = g.user['enterprise_id']
    user_id = g.user['id']
    
    try:
        cursor.execute("SELECT * FROM erp_comprobantes WHERE id = %s", (id_nc,))
        nc = cursor.fetchone()
        
        # Cuentas base
        codigos = ['1.3.01', '4.1', '2.2.01', '2.1.01', '2.2.03']
        cursor.execute("SELECT id, codigo FROM cont_plan_cuentas WHERE enterprise_id = %s AND codigo IN (%s)" % 
                       (enterprise_id, ",".join(["'%s'" % c for c in codigos])))
        cuentas = {row['codigo']: row['id'] for row in cursor.fetchall()}
        
        # Próximo número de asiento
        cursor.execute("SELECT COALESCE(MAX(numero_asiento), 0) + 1 as proximo FROM cont_asientos WHERE enterprise_id = %s", (enterprise_id,))
        nro_asiento = cursor.fetchone()['proximo']
        
        concepto = f"NC {nc['tipo_comprobante']} {nc['punto_venta']}-{nc['numero']} (Ref Fac {id_factura})"
        cursor.execute("""
            INSERT INTO cont_asientos (enterprise_id, fecha, concepto, modulo_origen, comprobante_id, numero_asiento, user_id)
            VALUES (%s, %s, %s, 'VENTAS', %s, %s, %s)
        """, (enterprise_id, nc['fecha_emision'], concepto, id_nc, nro_asiento, user_id))
        asiento_id = cursor.lastrowid
        
        neto = float(nc['importe_neto'])
        iva = float(nc['importe_iva'])
        total = float(nc['importe_total'])

        # 1. Ventas (Neto) al DEBE
        cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s)",
                       (asiento_id, cuentas['4.1'], neto, 0, enterprise_id, user_id))
        
        # 2. IVA al DEBE
        # Lógica de mes:
        cuenta_iva = cuentas.get('2.2.01') # Default DF
        if not es_mismo_mes and '2.1.01' in cuentas:
            cuenta_iva = cuentas['2.1.01'] # Crédito Fiscal si es otro mes
            
        if iva > 0:
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s)",
                           (asiento_id, cuenta_iva, iva, 0, enterprise_id, user_id))
            
        # 3. Deudores (Total) al HABER
        cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber, enterprise_id, user_id) VALUES (%s, %s, %s, %s, %s, %s)",
                       (asiento_id, cuentas['1.3.01'], 0, total, enterprise_id, user_id))
        
        return asiento_id
    except Exception as e:
        print(f"Error en asiento NC avanzado: {e}")
        return None

def _generar_asiento_contable(cursor, comprobante_id, enterprise_id, es_nc, user_id=None):
    """
    Genera el asiento contable para una factura o nota de credito.
    Busca las cuentas por codigo para asegurar compatibilidad multi-empresa.
    """
    try:
        # Obtener datos del comprobante
        cursor.execute("""
            SELECT importe_neto, importe_iva, importe_total, fecha_emision, tipo_comprobante, numero, punto_venta
            FROM erp_comprobantes WHERE id = %s AND enterprise_id = %s
        """, (comprobante_id, enterprise_id))
        c = cursor.fetchone()
        
        # Mapeo de Cuentas (Buscamos IDs por codigo)
        # 1.3.01: Deudores por Ventas, 4.1: Ventas, 2.2.01: IVA Debito Fiscal
        cursor.execute("SELECT id, codigo FROM cont_plan_cuentas WHERE enterprise_id = %s AND codigo IN ('1.3.01', '4.1', '2.2.01', '2.2.03')", (enterprise_id,))
        cuentas = {row['codigo']: row['id'] for row in cursor.fetchall()}
        
        # Validar que existan las cuentas necesarias (Deudores, Ventas, IVA, IIBB)
        requeridas = ['1.3.01', '4.1', '2.2.01', '2.2.03']
        faltantes = [k for k in requeridas if k not in cuentas]
        if faltantes:
            print(f"ERROR: Integridad contable fallida. Faltan cuentas: {faltantes} para la empresa {enterprise_id}")
            return None

        # Cabecera del Asiento
        cursor.execute("SELECT COALESCE(MAX(numero_asiento), 0) + 1 as proximo FROM cont_asientos WHERE enterprise_id = %s", (enterprise_id,))
        row_n = cursor.fetchone()
        proximo_nro_asiento = row_n['proximo'] if row_n else 1

        concepto = f"{'Nota de Crédito' if es_nc else 'Factura'} {c['tipo_comprobante']} {c['punto_venta']}-{c['numero']}"
        cursor.execute("""
            INSERT INTO cont_asientos (enterprise_id, fecha, concepto, modulo_origen, comprobante_id, numero_asiento, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (enterprise_id, c['fecha_emision'], concepto, 'VENTAS', comprobante_id, proximo_nro_asiento, user_id))
        asiento_id = cursor.lastrowid
        
        neto = float(c['importe_neto'])
        iva = float(c['importe_iva'])
        total = float(c['importe_total'])

        # Obtener total percepciones dinámicas
        cursor.execute("SELECT SUM(importe) as total FROM erp_comprobantes_impuestos WHERE comprobante_id = %s AND enterprise_id = %s", (comprobante_id, enterprise_id))
        perc_row = cursor.fetchone()
        total_perc = float(perc_row['total'] or 0)

        if not es_nc:
            # ASIENTO FACTURA:
            # Deudores por Venta (Total) al DEBE
            # Ventas (Neto) al HABER
            # IVA DF (IVA) al HABER
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber) VALUES (%s, %s, %s, %s)",
                           (asiento_id, cuentas['1.3.01'], total, 0))
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber) VALUES (%s, %s, %s, %s)",
                           (asiento_id, cuentas['4.1'], 0, neto))
            if iva > 0:
                cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber) VALUES (%s, %s, %s, %s)",
                               (asiento_id, cuentas['2.2.01'], 0, iva))
            if total_perc > 0:
                cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber) VALUES (%s, %s, %s, %s)",
                               (asiento_id, cuentas['2.2.03'], 0, total_perc))
        else:
            # ASIENTO NOTA DE CRÉDITO (Inverso):
            # Ventas (Neto) al DEBE
            # IVA DF (IVA) al DEBE
            # Deudores por Venta (Total) al HABER
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber) VALUES (%s, %s, %s, %s)",
                           (asiento_id, cuentas['4.1'], neto, 0))
            if iva > 0:
                cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber) VALUES (%s, %s, %s, %s)",
                               (asiento_id, cuentas['2.2.01'], iva, 0))
            if total_perc > 0:
                cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber) VALUES (%s, %s, %s, %s)",
                               (asiento_id, cuentas['2.2.03'], total_perc, 0))
            cursor.execute("INSERT INTO cont_asientos_detalle (asiento_id, cuenta_id, debe, haber) VALUES (%s, %s, %s, %s)",
                           (asiento_id, cuentas['1.3.01'], 0, total))
            
        return asiento_id
    except Exception as e:
        print(f"Error generando asiento: {e}")
        return None

@ventas_bp.route('/ventas/comprobante/<int:id>/reenviar', methods=['POST'])
@login_required
def reenviar_comprobante(id):
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Re-obtener datos para el PDF y el email
            cursor.execute("""
                SELECT erp_comprobantes.*, erp_terceros.nombre as cliente_nombre, erp_terceros.email as cliente_email, erp_terceros.cuit as cliente_cuit, erp_terceros.tipo_responsable as cliente_iva,
                       asoc.punto_venta as asoc_punto_venta, asoc.numero as asoc_numero
                FROM erp_comprobantes
                JOIN erp_terceros ON erp_comprobantes.tercero_id = erp_terceros.id
                LEFT JOIN erp_comprobantes AS asoc ON erp_comprobantes.comprobante_asociado_id = asoc.id
                WHERE erp_comprobantes.id = %s AND erp_comprobantes.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            comprobante = cursor.fetchone()
            
            if not comprobante:
                return jsonify({'success': False, 'message': 'Comprobante no encontrado'}), 404
                
            if not comprobante['cliente_email']:
                return jsonify({'success': False, 'message': 'El cliente no tiene un correo configurado'}), 400

            # Detalles (Usar LEFT JOIN para no excluir lineas sin articulo_id como servicios)
            cursor.execute("""
                SELECT erp_comprobantes_detalle.*, stk_articulos.nombre AS articulo_nombre, stk_articulos.codigo AS articulo_codigo
                FROM erp_comprobantes_detalle
                LEFT JOIN stk_articulos ON erp_comprobantes_detalle.articulo_id = stk_articulos.id
                WHERE erp_comprobantes_detalle.comprobante_id = %s AND erp_comprobantes_detalle.enterprise_id = %s
            """, (id, g.user['enterprise_id']))
            detalles = cursor.fetchall()
            
            # Dirección
            cursor.execute("SELECT * FROM erp_direcciones WHERE tercero_id = %s AND enterprise_id = %s AND es_fiscal = 1", (comprobante['tercero_id'], g.user['enterprise_id']))
            direccion = cursor.fetchone()
            
            # Impuestos / Percepciones
            cursor.execute("SELECT * FROM erp_comprobantes_impuestos WHERE comprobante_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            impuestos = cursor.fetchall()

            # Empresa
            cursor.execute("SELECT * FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
            empresa = cursor.fetchone()

        # Generar PDF 
        layout = BillingService.get_layout(g.user['enterprise_id'])
        vals = BillingService.prepare_invoice_values(comprobante, detalles, empresa, direccion, impuestos)
        html_pdf = render_template('ventas/comprobante_impresion.html', 
                               c=comprobante, detalles=detalles, cliente_dir=direccion, empresa=empresa, layout=layout, vals=vals, impuestos=impuestos)
        pdf_out = io.BytesIO()
        pisa.CreatePDF(io.StringIO(html_pdf), dest=pdf_out)
        pdf_content = pdf_out.getvalue()

        # Enviar Email
        fact_nro = f"{comprobante['punto_venta']:05d}-{comprobante['numero']:08d}"
        
        # Calcular total perc (si existe)
        total_perc = 0
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT SUM(importe) as total FROM erp_comprobantes_impuestos WHERE comprobante_id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            perc_row = cursor.fetchone()
            if perc_row:
                total_perc = float(perc_row['total'] or 0)

        subject, html_body = enviar_notificacion_percepcion(
            comprobante['cliente_email'], comprobante['cliente_nombre'], fact_nro, total_perc, g.user['enterprise_id']
        )

        pdf_filename = f"Factura_{fact_nro}.pdf"
        success, error = _enviar_email(comprobante['cliente_email'], subject, html_body, [(pdf_filename, pdf_content)], enterprise_id=g.user['enterprise_id'])
        
        if success:
            return jsonify({'success': True, 'message': f'✅ Comprobante re-enviado con éxito a {comprobante["cliente_email"]}'})
        else:
            return jsonify({'success': False, 'message': f"⚠️ Error al re-enviar: '{error}'. Verifique la configuración de correo o contacte al administrador."})

    except Exception as e:
        return jsonify({'success': False, 'message': f'Error inesperado: {str(e)}'}), 500

@ventas_bp.route('/api/clientes/<int:id>/cuenta_corriente')
@login_required
def api_cuenta_corriente(id):
    enterprise_id = g.user['enterprise_id']
    cursor = get_db_cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT
                erp_comprobantes.fecha_emision                              AS fecha,
                erp_comprobantes.tipo_comprobante                          AS tipo_doc,
                CONCAT(
                    LPAD(erp_comprobantes.punto_venta, 4, '0'), '-',
                    LPAD(erp_comprobantes.numero,      8, '0')
                )                                           AS nro_documento,
                NULL                                        AS nro_recibo,
                NULL                                        AS nro_doc_aplicado,
                erp_comprobantes.importe_total                             AS importe_bruto,
                erp_comprobantes.tipo_comprobante                          AS _signo_tipo,
                erp_comprobantes.id                                        AS comprobante_id,
                erp_comprobantes.asiento_id                                AS asiento_id,
                COALESCE((
                    SELECT SUM(erp_comprobantes_impuestos.importe)
                    FROM erp_comprobantes_impuestos
                    WHERE erp_comprobantes_impuestos.comprobante_id = erp_comprobantes.id
                      AND erp_comprobantes_impuestos.enterprise_id  = erp_comprobantes.enterprise_id
                ), 0)                                       AS total_percepciones,
                0                                           AS total_retenciones
            FROM erp_comprobantes
            WHERE erp_comprobantes.tercero_id = %s
              AND erp_comprobantes.enterprise_id = %s
              AND erp_comprobantes.modulo IN ('VEN', 'VENTAS')
        """, (id, enterprise_id))
        rows_comp = cursor.fetchall()

        cursor.execute("""
            SELECT
                fin_recibos.fecha                                     AS fecha,
                'REC'                                       AS tipo_doc,
                NULL                                        AS nro_documento,
                CONCAT(
                    LPAD(fin_recibos.punto_venta, 4, '0'), '-',
                    LPAD(fin_recibos.numero,      8, '0')
                )                                           AS nro_recibo,
                GROUP_CONCAT(
                    DISTINCT CONCAT(
                        LPAD(erp_comprobantes.punto_venta, 4, '0'), '-',
                        LPAD(erp_comprobantes.numero,      8, '0')
                    )
                    ORDER BY erp_comprobantes.numero
                    SEPARATOR ' / '
                )                                           AS nro_doc_aplicado,
                SUM(fin_recibos_detalles.importe)                             AS importe_bruto,
                'REC'                                       AS _signo_tipo,
                NULL                                        AS comprobante_id,
                fin_recibos.asiento_id                                AS asiento_id,
                0                                           AS total_percepciones,
                COALESCE((
                    SELECT SUM(fin_retenciones_emitidas.importe_retencion)
                    FROM fin_retenciones_emitidas
                    WHERE fin_retenciones_emitidas.comprobante_pago_id = fin_recibos.id
                      AND fin_retenciones_emitidas.enterprise_id = fin_recibos.enterprise_id
                ), 0)                                       AS total_retenciones
            FROM fin_recibos
            JOIN fin_recibos_detalles ON fin_recibos_detalles.recibo_id = fin_recibos.id
            JOIN erp_comprobantes      ON erp_comprobantes.id = fin_recibos_detalles.factura_id
            WHERE fin_recibos.tercero_id = %s
              AND fin_recibos.enterprise_id = %s
            GROUP BY fin_recibos.id, fin_recibos.fecha, fin_recibos.punto_venta, fin_recibos.numero, fin_recibos.asiento_id
        """, (id, enterprise_id))
        rows_rec = cursor.fetchall()

        DEBITO_TIPOS = {'001','002','006','007','011','012','005','010','015'}
        NC_TIPOS     = {'003','008','013'}

        cuenta_corriente = []
        saldo = 0.0
        for row in sorted(list(rows_comp) + list(rows_rec), key=lambda r: (r['fecha'] or '1900-01-01')):
            tipo = row['_signo_tipo']
            importe = float(row['importe_bruto'] or 0)

            if tipo in DEBITO_TIPOS:
                debe = importe
                haber = 0.0
                saldo += importe
            elif tipo in NC_TIPOS:
                debe = 0.0
                haber = importe
                saldo -= importe
            else:
                debe = 0.0
                haber = importe
                saldo -= importe

            cuenta_corriente.append({
                'fecha':              row['fecha'],
                'tipo_doc':           row['tipo_doc'],
                'nro_documento':      row['nro_documento'],
                'nro_recibo':         row['nro_recibo'],
                'nro_doc_aplicado':   row['nro_doc_aplicado'],
                'debe':               debe,
                'haber':              haber,
                'saldo':              saldo,
                'comprobante_id':     row['comprobante_id'],
                'asiento_id':         row['asiento_id'],
                'total_percepciones': float(row['total_percepciones']),
                'total_retenciones':  float(row['total_retenciones'])
            })
            
        cursor.close()
        return render_template('ventas/cuenta_corriente_modal.html', cuenta_corriente=cuenta_corriente)

    except Exception as e:
        cursor.close()
        current_app.logger.error(str(e))
        return f"<div class='alert alert-danger'>Error al cargar cuenta corriente</div>", 500
