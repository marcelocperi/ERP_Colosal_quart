
from flask import Blueprint, render_template, request, redirect, url_for, flash, g, jsonify, current_app
from database import get_db_cursor
from core.decorators import login_required, permission_required
from services import email_service, finance_service, library_api_service, system_service
import datetime
import threading
import logging
import re

biblioteca_bp = Blueprint('biblioteca', __name__, template_folder='templates', static_folder='static')
logger = logging.getLogger(__name__)

# --- HELPERS ---
def _async_email(func, *args):
    try:
        threading.Thread(target=func, args=args).start()
    except Exception as e:
        logger.error(f"Email Thread Error: {e}")

# --- DASHBOARD ---
@biblioteca_bp.route('/dashboard')
@login_required
def dashboard():
    try:
        sid = g.sid
        perms = g.permissions
        
        # ─── PROTECCIÓN 1: ASEGURAR ESTADO DE ENTERPRISE (ANTI-FREEZE) ──────────
        if not hasattr(g, 'enterprise') or not g.enterprise or not g.enterprise.get('nombre'):
            try:
                with get_db_cursor() as cursor:
                    cursor.execute("SELECT nombre, logo_path, lema FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
                    ent_row = cursor.fetchone()
                    if ent_row:
                        g.enterprise = {'nombre': ent_row[0], 'logo_path': ent_row[1], 'lema': ent_row[2]}
                    else:
                        g.enterprise = {'nombre': 'ERP Colosal', 'logo_path': None, 'lema': ''}
            except:
                g.enterprise = {'nombre': 'ERP Colosal', 'logo_path': None, 'lema': ''}
    
        # ─── PROTECCIÓN 2: ANÁLISIS SoD AISLADO ────────────────────────────────
        # Si el servicio SoD falla por datos corruptos, no debe bloquear el acceso
        try:
            from services.sod_service import analyze_role_sod
            import json
            
            with get_db_cursor(dictionary=True) as cursor:
                cursor.execute("SELECT name FROM sys_roles WHERE id = %s", (g.user['role_id'],))
                role_row = cursor.fetchone()
                role_name = role_row['name'] if role_row else "Default"
                
                cursor.execute("""
                    SELECT sys_permissions.id, sys_permissions.code, sys_permissions.description, sys_permissions.category 
                    FROM sys_permissions
                    JOIN sys_role_permissions ON sys_permissions.id = sys_role_permissions.permission_id
                    WHERE sys_role_permissions.role_id = %s AND sys_role_permissions.enterprise_id = %s
                """, (g.user['role_id'], g.user['enterprise_id']))
                perms_list = cursor.fetchall()
                
                sod_analysis = analyze_role_sod(role_name, perms_list)
                if sod_analysis.get('conflictos_detalle'):
                    error_data = {
                        'sod_error': True,
                        'is_login_warning': True,
                        'legend': "Control de Segregación de Funciones: Detectados conflictos de seguridad.",
                        'conflictos': []
                    }
                    for c in sod_analysis['conflictos_detalle']:
                        error_data['conflictos'].append({
                            'regla': c['regla'], 'detalle': c['detalle'], 'tipo': c.get('tipo', 'Conflicto'),
                            'perms': [{'code': p['code'], 'desc': p.get('description',''), 'cat': p.get('category','')} for p in c['perms']]
                        })
                    flash(json.dumps(error_data), "sod_danger")
        except Exception as e:
            logger.error(f"SILENT FAIL (SoD): {e}")
            
        # ─── EXTRA: VERIFICACIÓN DE INCIDENTES EN LOGS ──────────────────────────
        # Si el usuario es admin o puede ver logs, notificarle incidentes pendientes
        try:
            if any(p in ['sysadmin', 'admin_users', 'view_error_log', 'all'] for p in perms):
                with get_db_cursor(dictionary=True) as cursor:
                    cursor.execute("""
                        SELECT id, module, endpoint, failure_mode, error_message, impact_category, request_method
                        FROM sys_transaction_logs 
                        WHERE status = 'ERROR' 
                        AND (incident_status IS NULL OR incident_status != 'RESOLVED')
                        AND enterprise_id = %s
                        AND session_id = %s
                        ORDER BY created_at DESC
                        LIMIT 10
                    """, (g.user['enterprise_id'], sid))
                    incidentes = cursor.fetchall()
                    
                    if incidentes:
                        error_data = {
                            'sod_error': True,
                            'swal_icon': 'error',
                            'main_title': 'ALERTA DE SISTEMA: INCIDENTES PENDIENTES',
                            'sub_title': 'Detección Automática de Errores Críticos',
                            'icon': 'fa-triangle-exclamation',
                            'main_color': '#ef4444',
                            'bg_warning': 'rgba(239, 68, 68, 0.1)',
                            'text_warning': '#fca5a5',
                            'conflict_prefix': 'Fallo en',
                            'detail_label': 'DIAGNÓSTICO TÉCNICO',
                            'legend': f"El sistema ha interceptado {len(incidentes)} error(es) reciente(s) que requieren revisión técnica o administrativa. Se ha bloqueado el colapso de la aplicación de manera preventiva.",
                            'conflictos': []
                        }
                        
                        for error in incidentes:
                            error_data['conflictos'].append({
                                'tipo': error['module'] or 'General',
                                'regla': f"Terminal Origen: {error['endpoint']}",
                                'detalle': f"{error['error_message']} (Log ID: #{error['id']})",
                                'perms': [
                                    {
                                        'label_mod': 'Modo de Falla', 
                                        'cat': error['failure_mode'] or 'Transaccional',
                                        'label_obj': 'Impacto',
                                        'desc': error['impact_category'] or 'Desconocido'
                                    }
                                ]
                            })
                            
                        import json
                        flash(json.dumps(error_data), "sys_danger")
        except Exception as e:
            logger.error(f"SILENT FAIL (Logs check): {e}")
    
        # ─── PROTECCIÓN 3: DISPATCHER SEGURO ──────────────────────────────────
        # Priority Redirection: Siempre el camino más específico primero
        try:
            if any(p.startswith('compras') or p in ['purchases_view', 'compras_view', 'all'] for p in perms):
                return redirect(url_for('compras.dashboard'))
            elif any(p.startswith('ventas') or p in ['sales_view', 'ventas_view'] for p in perms):
                return redirect(url_for('ventas.dashboard'))
            elif any(p.startswith('fondos') or p in ['treasury_view', 'fondos_view'] for p in perms):
                return redirect(url_for('fondos.dashboard'))
            elif any(p.startswith('stock') or p in ['stock_view', 'inventario_view'] for p in perms):
                return redirect(url_for('stock.dashboard'))
            elif any(p.startswith('contabilidad') or p in ['contabilidad_view'] for p in perms):
                return redirect(url_for('contabilidad.dashboard'))
            elif any(p.startswith('produccion') or p in ['produccion_view'] for p in perms):
                return redirect(url_for('produccion.dashboard'))
            elif any(p.startswith('pricing') or p in ['pricing_view'] for p in perms):
                return redirect(url_for('pricing.dashboard'))
            elif any(p.startswith('utilitarios') or p in ['utilitarios_view'] for p in perms):
                return redirect(url_for('utilitarios.gestor_crons'))
            elif any(p.startswith('admin') or p in ['admin_users', 'admin_roles'] for p in perms):
                return redirect(url_for('core.admin_users'))
            elif any(p.startswith('patrons') or p.startswith('biblioteca') for p in perms):
                return await render_template('dashboard.html')
                
            # Si llegamos aquí y hay permisos, permitimos ver el dashboard básico por defecto
            if perms:
                return await render_template('dashboard.html')
        except Exception as e:
            logger.error(f"DISPATCH ERROR: {e}")
            return await render_template('dashboard.html')
    
        # Fallback final solo si realmente no tiene nada
        flash("Configuración de acceso incompleta. Contacte a soporte.", "warning")
        return redirect(url_for('core.logout'))
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f"Error general en el dashboard: {str(e)}", "danger")
        return await render_template('dashboard.html')


@biblioteca_bp.route('/api/dolar/refresh')
@login_required
def refresh_dolar():
    res = finance_service.obtener_y_guardar_cotizacion(origen='operador')
    if res:
        flash("Cotización actualizada manualmente", "success")
    else:
        flash("Error al actualizar cotización", "danger")
    return redirect(url_for('biblioteca.dashboard'))

# --- USUARIOS (PATRONS) ---
# NOTA: La gestión de artículos/libros se realiza desde /stock/articulos (módulo Stock/ERP)

@biblioteca_bp.route('/usuarios')
@login_required
@permission_required('patrons_view')
def usuarios():
    with get_db_cursor() as cursor:
        cursor.execute("SELECT id, nombre, apellido, telefono, email FROM usuarios WHERE enterprise_id = %s ORDER BY apellido ASC", (g.user['enterprise_id'],))
        rows = cursor.fetchall()
    
    # We map to dictionaries for easier template handling if needed, 
    # but usuarios.html is already using dot notation or indexes%s 
    # Let's check usuarios.html: it uses u.id, u.nombre, etc.
    final_users = []
    for r in rows:
        final_users.append({'id': r[0], 'nombre': r[1], 'apellido': r[2], 'telefono': r[3], 'email': r[4]})
        
    return await render_template('usuarios.html', usuarios=final_users)

@biblioteca_bp.route('/usuarios/agregar', methods=['POST'])
@login_required
@permission_required('patrons_manage')
def agregar_usuario():
    try:
        email = request.form['email']
        # Validar estado del correo
        es_valido, msg = email_service.validar_estado_correo(email)
        if not es_valido:
            flash(f"Error de Validación de Correo: {msg}", "danger")
            return redirect(url_for('biblioteca.usuarios'))

        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO usuarios 
                (enterprise_id, nombre, apellido, email, telefono, created_by) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (g.user['enterprise_id'], request.form['nombre'], request.form['apellido'], 
                  email, request.form.get('telefono'), g.user['id']))
        flash("Usuario registrado", "success")
    except Exception as e:
        flash(f"Error: {e}", "danger")
    return redirect(url_for('biblioteca.usuarios'))

@biblioteca_bp.route('/usuarios/modificar/<int:id>', methods=['POST'])
@login_required
@permission_required('patrons_manage')
def modificar_usuario(id):
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                UPDATE usuarios 
                SET nombre=%s, apellido=%s, email=%s, telefono=%s, updated_by=%s, updated_at=CURRENT_TIMESTAMP 
                WHERE id=%s AND enterprise_id=%s
            """, (request.form['nombre'], request.form['apellido'], request.form['email'], 
                  request.form['telefono'], g.user['id'], id, g.user['enterprise_id']))
        flash("Usuario actualizado", "success")
    except Exception as e:
        flash(f"Error: {e}", "danger")
    return redirect(url_for('biblioteca.usuarios'))

# --- PRÉSTAMOS ---

@biblioteca_bp.route('/prestamos')
@login_required
@permission_required('loans_view')
def prestamos():
    with get_db_cursor() as cursor:
        # Activos
        cursor.execute("""
            SELECT prestamos.id, usuarios.nombre, usuarios.apellido, stk_articulos.nombre, prestamos.fecha_prestamo, prestamos.fecha_devol_esperada, prestamos.estado,
                   usuarios.email, stk_articulos.codigo as isbn, stk_articulos.modelo as autor, usuarios.telefono
            FROM prestamos
            JOIN usuarios ON prestamos.usuario_id = usuarios.id AND usuarios.enterprise_id = prestamos.enterprise_id
            JOIN stk_articulos ON prestamos.libro_id = stk_articulos.id AND stk_articulos.enterprise_id = prestamos.enterprise_id
            WHERE prestamos.fecha_devolucion_real IS NULL AND prestamos.enterprise_id = %s
            ORDER BY prestamos.fecha_prestamo DESC
        """, (g.user['enterprise_id'],))
        prestamos = cursor.fetchall()
        
        # Load lists for modal
        # Current template expects: u[0]=id, u[1]=email, u[2]=nombre, u[3]=apellido for option displays
        cursor.execute("SELECT id, email, nombre, apellido FROM usuarios WHERE enterprise_id = %s ORDER BY apellido", (g.user['enterprise_id'],))
        usuarios_list = cursor.fetchall()
        
        cursor.execute("SELECT id, nombre, codigo as isbn FROM stk_articulos WHERE enterprise_id = %s ORDER BY nombre", (g.user['enterprise_id'],))
        libros_list = cursor.fetchall()
        
    return await render_template('prestamos.html', prestamos=prestamos, usuarios=usuarios_list, libros=libros_list)

@biblioteca_bp.route('/prestamos/buscar')
@login_required
@permission_required('loans_view')
def buscar_prestamos():
    """Advanced search for active loans by multiple criteria"""
    titulo = request.args.get('titulo', '').strip()
    autor = request.args.get('autor', '').strip()
    isbn = request.args.get('isbn', '').strip()
    editorial = request.args.get('editorial', '').strip()
    usuario = request.args.get('usuario', '').strip()
    email = request.args.get('email', '').strip()
    telefono = request.args.get('telefono', '').strip()
    
    # Build dynamic query
    base_query = """
        SELECT prestamos.id, usuarios.nombre, usuarios.apellido, stk_articulos.nombre, prestamos.fecha_prestamo, prestamos.fecha_devol_esperada,
               usuarios.email, stk_articulos.codigo as isbn, stk_articulos.modelo as autor, usuarios.telefono, stk_articulos.marca as editorial
        FROM prestamos
        JOIN usuarios ON prestamos.usuario_id = usuarios.id AND usuarios.enterprise_id = prestamos.enterprise_id
        JOIN stk_articulos ON prestamos.libro_id = stk_articulos.id AND stk_articulos.enterprise_id = prestamos.enterprise_id
        WHERE prestamos.fecha_devolucion_real IS NULL AND prestamos.enterprise_id = %s
    """
    
    conditions = []
    params = [g.user['enterprise_id']]
    
    if titulo:
        conditions.append("stk_articulos.nombre LIKE %s")
        params.append(f"%{titulo}%")
    if autor:
        conditions.append("stk_articulos.modelo LIKE %s")
        params.append(f"%{autor}%")
    if isbn:
        conditions.append("stk_articulos.codigo LIKE %s")
        params.append(f"%{isbn}%")
    if editorial:
        conditions.append("stk_articulos.marca LIKE %s")
        params.append(f"%{editorial}%")
    if usuario:
        conditions.append("(usuarios.nombre LIKE %s OR usuarios.apellido LIKE %s)")
        params.extend([f"%{usuario}%", f"%{usuario}%"])
    if email:
        conditions.append("usuarios.email LIKE %s")
        params.append(f"%{email}%")
    if telefono:
        conditions.append("usuarios.telefono LIKE %s")
        params.append(f"%{telefono}%")
    
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
    
    base_query += " ORDER BY prestamos.fecha_prestamo DESC LIMIT 100"
    
    try:
        # Debug logging
        logger.info(f"Search params - titulo: {titulo}, autor: {autor}, isbn: {isbn}, editorial: {editorial}, usuario: {usuario}, email: {email}, telefono: {telefono}")
        logger.info(f"Query: {base_query}")
        logger.info(f"Params: {params}")
        
        with get_db_cursor() as cursor:
            cursor.execute(base_query, params)
            rows = cursor.fetchall()
            logger.info(f"Found {len(rows)} results")
        
        prestamos = []
        for r in rows:
            prestamos.append({
                'id': r[0],
                'usuario_nombre': r[1],
                'usuario_apellido': r[2],
                'libro_titulo': r[3],
                'fecha_prestamo': str(r[4]),
                'fecha_devolucion': str(r[5]),
                'usuario_email': r[6],
                'libro_isbn': r[7],
                'libro_autor': r[8],
                'usuario_telefono': r[9] or '',
                'libro_editorial': r[10] or ''
            })
        
        return jsonify({'prestamos': prestamos})
    except Exception as e:
        logger.error(f"Error searching loans: {e}")
        return jsonify({'error': str(e)}), 500

@biblioteca_bp.route('/prestamos/registrar', methods=['POST'])
@login_required
@permission_required('loans_manage')
def registrar_prestamo():
    libro_id = request.form['libro_id']
    usuario_id = request.form['usuario_id']
    dias = request.form.get('dias')
    fecha_dev_str = request.form.get('fecha_devol_esperada')
    
    try:
        if fecha_dev_str:
            fecha_dev = datetime.datetime.strptime(fecha_dev_str, '%Y-%m-%d').date()
        else:
            fecha_dev = datetime.date.today() + datetime.timedelta(days=int(dias or 14))
        with get_db_cursor() as cursor:
            # 1. Verificar si el usuario ya tiene este libro prestado
            cursor.execute("""
                SELECT COUNT(*) FROM prestamos 
                WHERE usuario_id = %s AND libro_id = %s 
                AND fecha_devolucion_real IS NULL 
                AND enterprise_id = %s
            """, (usuario_id, libro_id, g.user['enterprise_id']))
            ya_prestado = cursor.fetchone()[0]
            
            if ya_prestado > 0:
                flash("Este usuario ya tiene un préstamo activo de este libro", "danger")
                return redirect(url_for('biblioteca.prestamos'))

            # 1.5. CHECK: Verificar Reservas Activas
            cursor.execute("""
                SELECT movimientos_pendientes.id, movimientos_pendientes.comentario
                FROM movimientos_pendientes
                JOIN stock_motivos ON movimientos_pendientes.motivo_id = stock_motivos.id
                WHERE movimientos_pendientes.libro_id = %s AND movimientos_pendientes.estado = 'pendiente' 
                AND (stock_motivos.system_code = 'RESERVE' OR stock_motivos.descripcion LIKE 'Reserva%%')
                AND movimientos_pendientes.enterprise_id = %s
                ORDER BY movimientos_pendientes.fecha_registro ASC
                LIMIT 1
            """, (libro_id, g.user['enterprise_id']))
            reserva_activa = cursor.fetchone()
            
            reserva_id_to_complete = None
            
            if reserva_activa:
                rid, comentario_json = reserva_activa
                import json
                try:
                    c_data = json.loads(comentario_json)
                    r_user_id = str(c_data.get('user_id'))
                    
                    if str(usuario_id) != r_user_id:
                        r_name = c_data.get('user_name', 'otro usuario')
                        flash(f"No se puede prestar: Este libro está RESERVADO por {r_name}.", "danger")
                        return redirect(url_for('biblioteca.prestamos'))
                    else:
                        # Es el usuario correcto -> Marcaremos la reserva como completada tras el éxito del préstamo
                        reserva_id_to_complete = rid
                except:
                    pass
            
            # 2. Verificar si el usuario tiene deudas pendientes
            cursor.execute("""
                SELECT prestamos.id, prestamos.deuda, prestamos.fecha_devol_esperada, stk_articulos.nombre as libro_nombre
                FROM prestamos
                JOIN stk_articulos ON prestamos.libro_id = stk_articulos.id AND stk_articulos.enterprise_id = prestamos.enterprise_id
                WHERE prestamos.usuario_id = %s AND prestamos.enterprise_id = %s AND prestamos.deuda > 0
                ORDER BY prestamos.fecha_devol_esperada ASC
            """, (usuario_id, g.user['enterprise_id']))
            deudas = cursor.fetchall()
            
            if deudas:
                deuda_total = sum([d[1] for d in deudas])
                # Get info about the earliest due loan
                primera_deuda = deudas[0]
                fecha_limite = primera_deuda[2]
                libro_con_deuda = primera_deuda[3]
                
                flash(f"Este usuario tiene una deuda pendiente de ${deuda_total:.2f}. Libro con deuda: '{libro_con_deuda}'. No puede solicitar nuevos préstamos hasta saldar el saldo. Fecha límite original: {fecha_limite}", "danger")
                return redirect(url_for('biblioteca.prestamos'))
            
            # 3. Validar disponibilidad real (Stock físico en stk_existencias - Préstamos Activos - Egresos/Bajas pendientes)
            cursor.execute("""
                SELECT (
                    IFNULL((SELECT SUM(cantidad) FROM stk_existencias WHERE articulo_id = stk_articulos.id AND enterprise_id = stk_articulos.enterprise_id), 0) - 
                    IFNULL((SELECT COUNT(*) FROM prestamos 
                           WHERE prestamos.libro_id = stk_articulos.id 
                           AND prestamos.fecha_devolucion_real IS NULL 
                           AND prestamos.enterprise_id = stk_articulos.enterprise_id), 0) -
                    IFNULL((SELECT SUM(movimientos_pendientes.cantidad) FROM movimientos_pendientes 
                           WHERE movimientos_pendientes.libro_id = stk_articulos.id 
                           AND (movimientos_pendientes.tipo = 'egreso' OR movimientos_pendientes.tipo = 'baja') 
                           AND movimientos_pendientes.estado = 'pendiente' AND movimientos_pendientes.enterprise_id = stk_articulos.enterprise_id), 0)) as disponible
                FROM stk_articulos WHERE stk_articulos.id = %s AND stk_articulos.enterprise_id = %s
            """, (libro_id, g.user['enterprise_id']))
            res_stock = cursor.fetchone()
            disponible = res_stock[0] if res_stock else 0
            
            if disponible <= 0:
                # Buscar información de quién lo tiene y cuándo vuelve (Próxima devolución)
                cursor.execute("""
                    SELECT usuarios.nombre, usuarios.apellido, prestamos.fecha_devol_esperada 
                    FROM prestamos
                    JOIN usuarios ON prestamos.usuario_id = usuarios.id AND usuarios.enterprise_id = prestamos.enterprise_id
                    WHERE prestamos.libro_id = %s AND prestamos.fecha_devolucion_real IS NULL AND prestamos.enterprise_id = %s
                    ORDER BY prestamos.fecha_devol_esperada ASC LIMIT 1
                """, (libro_id, g.user['enterprise_id']))
                info_prestamo = cursor.fetchone()
                
                msg = "No hay ejemplares disponibles (Stock agotado)"
                if info_prestamo:
                    tenedor = f"{info_prestamo[0]} {info_prestamo[1]}"
                    fecha_vuelta = info_prestamo[2]
                    # Format date nicely if possible, or leave as is
                    msg = f"No hay saldo disponible. El libro está en poder de {tenedor} y se espera su devolución el {fecha_vuelta}."
                
                flash(msg, "warning")
                return redirect(url_for('biblioteca.prestamos', reservar_libro=libro_id, reservar_usuario=usuario_id))
            
            # 2. Obtener Motivo de Stock (ERP)
            cursor.execute("SELECT id FROM stk_motivos WHERE system_code = 'LOAN_OUT' AND enterprise_id = %s LIMIT 1", (g.user['enterprise_id'],))
            motivo_row = cursor.fetchone()
            if not motivo_row:
                 # Fallback
                 cursor.execute("SELECT id FROM stk_motivos WHERE nombre LIKE 'Préstamo%%' AND enterprise_id = %s LIMIT 1", (g.user['enterprise_id'],))
                 motivo_row = cursor.fetchone()
                 if not motivo_row: raise Exception("Configuración faltante: No existe el motivo de salida por préstamo (system_code: LOAN_OUT)")
            motivo_stock_id = motivo_row[0]

            # 3. Registrar Préstamo
            cursor.execute("""
                INSERT INTO prestamos 
                (enterprise_id, libro_id, usuario_id, fecha_prestamo, fecha_devol_esperada, estado, user_id)
                VALUES (%s, %s, %s, CURDATE(), %s, 'activo', %s)
            """, (g.user['enterprise_id'], libro_id, usuario_id, fecha_dev, g.user['id']))
            prestamo_id = cursor.lastrowid
            
            # 4. Registrar movimiento de stock (Log ERP), sin descontar inventario Total
            # Default deposit for log context
            cursor.execute("SELECT id FROM stk_depositos WHERE enterprise_id = %s LIMIT 1", (g.user['enterprise_id'],))
            d_row = cursor.fetchone()
            dep_id = d_row[0] if d_row else 1
            
            cursor.execute("""
                INSERT INTO stk_movimientos (enterprise_id, fecha, motivo_id, deposito_origen_id, user_id, observaciones)
                VALUES (%s, NOW(), %s, %s, %s, 'Préstamo registrado (Salida temporal)')
            """, (g.user['enterprise_id'], motivo_stock_id, dep_id, g.user['id']))
            nm_id = cursor.lastrowid
            
            cursor.execute("INSERT INTO stk_movimientos_detalle (movimiento_id, articulo_id, cantidad) VALUES (%s, %s, 1)", (nm_id, libro_id))

            
            # Email
            cursor.execute("SELECT email, nombre FROM usuarios WHERE id = %s AND enterprise_id = %s", (usuario_id, g.user['enterprise_id']))
            u_row = cursor.fetchone()
            cursor.execute("SELECT nombre FROM stk_articulos WHERE id = %s AND enterprise_id = %s", (libro_id, g.user['enterprise_id']))
            l_row = cursor.fetchone()
            if u_row and l_row:
                # Need ISBN for email
                cursor.execute("SELECT codigo as isbn FROM stk_articulos WHERE id = %s AND enterprise_id = %s", (libro_id, g.user['enterprise_id']))
                isbn_row = cursor.fetchone()
                isbn = isbn_row[0] if isbn_row else '-'
                _async_email(email_service.enviar_notificacion_prestamo, u_row[0], u_row[1], l_row[0], isbn, str(fecha_dev), prestamo_id, g.user['enterprise_id'])

            # COMPLETAR RESERVA si existía
            if reserva_id_to_complete:
                cursor.execute("UPDATE movimientos_pendientes SET estado='completado' WHERE id=%s", (reserva_id_to_complete,))

        flash("Préstamo registrado exitosamente", "success")
    except Exception as e:
        flash(f"Error: {e}", "danger")
    return redirect(url_for('biblioteca.prestamos'))

@biblioteca_bp.route('/prestamos/devolver/<int:id>', methods=['POST'])
@login_required
@permission_required('loans_manage')
def devolver_prestamo(id):
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT fecha_prestamo, libro_id, usuario_id FROM prestamos WHERE id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            row = cursor.fetchone()
            if not row: raise Exception("Préstamo no encontrado")
            
            fecha_prestamo, libro_id, usuario_id = row
            fecha_dev_real = datetime.date.today()
            
            # 2. Obtener Motivo de Stock (ERP)
            cursor.execute("SELECT id FROM stk_motivos WHERE system_code = 'LOAN_IN' AND enterprise_id = %s LIMIT 1", (g.user['enterprise_id'],))
            motivo_row = cursor.fetchone()
            if not motivo_row:
                cursor.execute("SELECT id FROM stk_motivos WHERE nombre LIKE 'Devolución%%' AND enterprise_id = %s LIMIT 1", (g.user['enterprise_id'],))
                motivo_row = cursor.fetchone()
                if not motivo_row:
                    raise Exception("Configuración faltante: No existe el motivo de entrada por devolución (system_code: LOAN_IN)")
            motivo_stock_id = motivo_row[0]

            # 3. Registrar Devolución en Préstamo
            cursor.execute("""
                UPDATE prestamos 
                SET fecha_devolucion_real = %s, estado = 'devuelto', user_id_update = %s, updated_at = CURRENT_TIMESTAMP 
                WHERE id = %s AND enterprise_id = %s
            """, (fecha_dev_real, g.user['id'], id, g.user['enterprise_id']))
            
            # 4. Registrar movimiento de stock (Log ERP)
            # Default deposit for log context
            cursor.execute("SELECT id FROM stk_depositos WHERE enterprise_id = %s LIMIT 1", (g.user['enterprise_id'],))
            d_row = cursor.fetchone()
            dep_id = d_row[0] if d_row else 1

            cursor.execute("""
                INSERT INTO stk_movimientos (enterprise_id, fecha, motivo_id, deposito_destino_id, user_id, observaciones)
                VALUES (%s, NOW(), %s, %s, %s, 'Devolución de préstamo (Reingreso)')
            """, (g.user['enterprise_id'], motivo_stock_id, dep_id, g.user['id']))
            nm_id = cursor.lastrowid
            
            cursor.execute("INSERT INTO stk_movimientos_detalle (movimiento_id, articulo_id, cantidad) VALUES (%s, %s, 1)", (nm_id, libro_id))
            
            # Finance: Check late fees
            try:
                # Fetch details for email
                cursor.execute("""
                    SELECT usuarios.email, usuarios.nombre, stk_articulos.nombre, prestamos.fecha_prestamo, stk_articulos.modelo as autor, stk_articulos.codigo as isbn 
                    FROM prestamos 
                    JOIN usuarios ON prestamos.usuario_id = usuarios.id AND usuarios.enterprise_id = prestamos.enterprise_id
                    JOIN stk_articulos ON prestamos.libro_id = stk_articulos.id AND stk_articulos.enterprise_id = prestamos.enterprise_id
                    WHERE prestamos.id = %s AND prestamos.enterprise_id = %s
                """, (id, g.user['enterprise_id']))
                mail_data = cursor.fetchone()
                if mail_data:
                    l_data = {
                        'titulo': mail_data[2],
                        'autor': mail_data[4] or '',
                        'isbn': mail_data[5] or ''
                    }
                    _async_email(email_service.enviar_notificacion_devolucion, mail_data[0], mail_data[1], l_data, str(mail_data[3]), id, g.user['enterprise_id'])
            except Exception as e:
                logger.error(f"Error en notificación de devolución: {e}")
            
            # CHECK RESERVATIONS & NOTIFY
            cursor.execute("""
                SELECT movimientos_pendientes.id, movimientos_pendientes.comentario, stk_articulos.nombre, stk_articulos.modelo as autor, stk_articulos.codigo as isbn
                FROM movimientos_pendientes
                JOIN stock_motivos ON movimientos_pendientes.motivo_id = stock_motivos.id
                JOIN stk_articulos ON movimientos_pendientes.libro_id = stk_articulos.id
                WHERE movimientos_pendientes.libro_id = %s AND movimientos_pendientes.estado = 'pendiente' 
                AND (stock_motivos.system_code = 'RESERVE' OR stock_motivos.descripcion LIKE 'Reserva%') 
                AND movimientos_pendientes.enterprise_id = %s
                ORDER BY movimientos_pendientes.fecha_registro ASC
                LIMIT 1
            """, (libro_id, g.user['enterprise_id']))
            reserva = cursor.fetchone()
            
            if reserva:
                reserva_id, comentario_json, r_titulo, r_autor, r_isbn = reserva
                import json
                try:
                    c_data = json.loads(comentario_json)
                    r_user_id = c_data.get('user_id')
                    
                    if r_user_id:
                        cursor.execute("SELECT email, nombre FROM usuarios WHERE id = %s AND enterprise_id = %s", (r_user_id, g.user['enterprise_id']))
                        r_user = cursor.fetchone()
                        
                        if r_user and r_user[0]:
                            # Marcar disponibilidad inyectando fecha en comentario
                            fecha_limite = datetime.datetime.now() + datetime.timedelta(hours=48)
                            c_data['available_since'] = str(datetime.datetime.now())
                            c_data['expires_at'] = str(fecha_limite)
                            
                            new_comment = json.dumps(c_data)
                            cursor.execute("UPDATE movimientos_pendientes SET comentario = %s WHERE id = %s", (new_comment, reserva_id))
                            
                            # Enviar email Notificación Disponibilidad
                            app_ctx = current_app._get_current_object()
                            ent_id = g.user['enterprise_id']
                            r_email = r_user[0]
                            r_name = r_user[1]
                            
                            l_data = {
                                'titulo': r_titulo,
                                'autor': r_autor or '',
                                'isbn': r_isbn or ''
                            }
                            
                            def send_avail_async(app, email, nombre, l_data, limite, eid):
                                with app.app_context():
                                    email_service.enviar_disponibilidad_reserva(
                                        email, nombre, l_data, limite.strftime("%d/%m/%Y %H:%M"), eid
                                    )
                            threading.Thread(target=send_avail_async, args=(app_ctx, r_email, r_name, l_data, fecha_limite, ent_id)).start()
                            
                            flash(f"ATENCIÓN: Este libro tiene una RESERVA activa para {r_name}. Se le ha notificado que puede retirarlo hasta {fecha_limite.strftime('%d/%m %H:%M')}.", "info")
                except Exception as e:
                    logger.error(f"Error procesando reserva en devolución: {e}")

        flash("Devolución registrada", "success")
    except Exception as e:
        flash(f"Error: {e}", "danger")
    return redirect(url_for('biblioteca.prestamos'))

# --- API ---
@biblioteca_bp.route('/api/libros/meta')
@login_required
def api_meta():
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT DISTINCT modelo FROM stk_articulos WHERE modelo IS NOT NULL AND enterprise_id = %s ORDER BY modelo", (g.user['enterprise_id'],))
            autores = [r[0] for r in cursor.fetchall()]
            cursor.execute("SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.genero')) FROM stk_articulos WHERE metadata_json IS NOT NULL AND enterprise_id = %s ORDER BY 1", (g.user['enterprise_id'],))
            generos = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute("SELECT DISTINCT marca FROM stk_articulos WHERE marca IS NOT NULL AND enterprise_id = %s ORDER BY marca", (g.user['enterprise_id'],))
            editoriales = [r[0] for r in cursor.fetchall()]
            return jsonify({'autores': autores, 'generos': generos, 'editoriales': editoriales})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@biblioteca_bp.route('/prestamos/reservar', methods=['POST'])
@login_required
@permission_required('loans_manage')
def reservar_libro():
    try:
        data = request.json
        libro_id = data.get('libro_id')
        usuario_id = data.get('usuario_id')
        
        with get_db_cursor() as cursor:
            # Check for 'Reserva' motive
            cursor.execute("SELECT id FROM stock_motivos WHERE system_code = 'RESERVE' AND enterprise_id = %s", (g.user['enterprise_id'],))
            motivo = cursor.fetchone()
            if not motivo:
                # Create if not exists - 'tipo' must be 'baja' (matches ENUM('alta', 'baja'))
                cursor.execute("INSERT INTO stock_motivos (enterprise_id, descripcion, tipo, es_pendiente, system_code) VALUES (%s, 'Reserva', 'baja', 1, 'RESERVE')", (g.user['enterprise_id'],))
                motivo_id = cursor.lastrowid
            else:
                motivo_id = motivo[0]

            # 1. Verificar si YA existe reserva para este libro (Limite 1 por libro)
            cursor.execute("""
                SELECT movimientos_pendientes.id FROM movimientos_pendientes
                JOIN stock_motivos ON movimientos_pendientes.motivo_id = stock_motivos.id
                WHERE movimientos_pendientes.libro_id = %s AND movimientos_pendientes.estado = 'pendiente' 
                AND (stock_motivos.system_code = 'RESERVE' OR stock_motivos.descripcion LIKE 'Reserva%%') AND movimientos_pendientes.enterprise_id = %s
            """, (libro_id, g.user['enterprise_id']))
            if cursor.fetchone():
                return jsonify({'success': False, 'error': 'Ya existe una reserva activa para este libro. Solo se permite una reserva a la vez.'})

            # Get User Info for display and email
            cursor.execute("SELECT nombre, apellido, email FROM usuarios WHERE id = %s AND enterprise_id = %s", (usuario_id, g.user['enterprise_id']))
            u = cursor.fetchone()
            user_display = f"{u[0]} {u[1]}" if u else "Desconocido"
            user_email = u[2] if u else None

            # 2. Calcular fecha estimada (cuando vuelve el próximo libro)
            cursor.execute("""
                SELECT MIN(fecha_devol_esperada) FROM prestamos 
                WHERE libro_id = %s AND fecha_devolucion_real IS NULL AND enterprise_id = %s
            """, (libro_id, g.user['enterprise_id']))
            row_fecha = cursor.fetchone()
            fecha_estimada = row_fecha[0] if row_fecha and row_fecha[0] else datetime.date.today()

            # Insert Pending Movement with JSON metadata in comment
            import json
            comentario_data = {
                "action": "reserve",
                "user_id": usuario_id,
                "user_name": user_display,
                "created_at": str(datetime.date.today())
            }
            
            cursor.execute("""
                INSERT INTO movimientos_pendientes 
                (enterprise_id, libro_id, motivo_id, tipo, cantidad, fecha_estimada, estado, comentario)
                VALUES (%s, %s, %s, 'egreso', 1, %s, 'pendiente', %s)
            """, (g.user['enterprise_id'], libro_id, motivo_id, fecha_estimada, json.dumps(comentario_data)))
            
            # 3. Enviar EMAIL Confirmación
            if user_email:
                cursor.execute("SELECT nombre, modelo as autor, codigo as isbn FROM stk_articulos WHERE id = %s AND enterprise_id = %s", (libro_id, g.user['enterprise_id']))
                lb_row = cursor.fetchone()
                libro_data = {
                    'titulo': lb_row[0],
                    'autor': lb_row[1] or '-',
                    'isbn': lb_row[2] or '-'
                }
                
                # Capture context for thread
                app_ctx = current_app._get_current_object()
                ent_id = g.user['enterprise_id']
                
                def send_async(app, email, nombre, l_data, fecha, eid):
                    with app.app_context():
                        email_service.enviar_confirmacion_reserva(email, nombre, l_data, str(fecha), eid)
                
                threading.Thread(target=send_async, args=(app_ctx, user_email, user_display, libro_data, fecha_estimada, ent_id)).start()

            return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@biblioteca_bp.route('/api/prestamos/reservas')
@login_required
def api_reservas():
    try:
        with get_db_cursor() as cursor:
             cursor.execute("""
                SELECT movimientos_pendientes.id, stk_articulos.nombre, movimientos_pendientes.fecha_estimada, movimientos_pendientes.comentario
                FROM movimientos_pendientes
                JOIN stk_articulos ON movimientos_pendientes.libro_id = stk_articulos.id AND stk_articulos.enterprise_id = movimientos_pendientes.enterprise_id
                JOIN stock_motivos ON movimientos_pendientes.motivo_id = stock_motivos.id AND stock_motivos.enterprise_id = movimientos_pendientes.enterprise_id
                WHERE movimientos_pendientes.estado = 'pendiente' 
                AND (stock_motivos.system_code = 'RESERVE' OR stock_motivos.descripcion LIKE 'Reserva%%')
                AND movimientos_pendientes.enterprise_id = %s
                ORDER BY movimientos_pendientes.fecha_estimada ASC
             """, (g.user['enterprise_id'],))
             rows = cursor.fetchall()
             
             import json
             reservas = []
             for r in rows:
                 usuario = "Desconocido"
                 try:
                     # Try parsing JSON
                     if r[3] and r[3].strip().startswith('{'):
                        c_data = json.loads(r[3])
                        usuario = c_data.get('user_name', 'Desconocido')
                     else:
                        # Fallback for old/manual
                        usuario = r[3]
                 except:
                     usuario = r[3]
                 
                 reservas.append({
                     'id': r[0],
                     'libro': r[1],
                     'fecha': str(r[2]),
                     'usuario': usuario
                 })
             return jsonify({'reservas': reservas})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@biblioteca_bp.route('/prestamos/reservas/anular/<int:id>', methods=['POST'])
@login_required
@permission_required('loans_manage')
def anular_reserva(id):
    motivo = request.form.get('motivo_anulacion')
    fecha = request.form.get('fecha_anulacion')
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT comentario FROM movimientos_pendientes WHERE id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
            row = cursor.fetchone()
            if row:
                current = row[0]
                new_comment = f"{current} | ANULADO: {motivo} ({fecha})"
                
                cursor.execute("UPDATE movimientos_pendientes SET estado = 'cancelado', comentario = %s WHERE id = %s AND enterprise_id = %s",
                            (new_comment, id, g.user['enterprise_id']))
                
                # ENVIAR EMAIL DE CANCELACIÓN
                import json
                try:
                    c_data = {}
                    if current.strip().startswith('{'):
                         c_data = json.loads(current)
                         user_id = c_data.get('user_id')
                         if user_id:
                             # Get User and Book Data
                             cursor.execute("SELECT email, nombre FROM usuarios WHERE id = %s AND enterprise_id = %s", (user_id, g.user['enterprise_id']))
                             u_row = cursor.fetchone()
                             
                             cursor.execute("""
                                SELECT stk_articulos.nombre, stk_articulos.modelo as autor, stk_articulos.codigo as isbn 
                                FROM movimientos_pendientes
                                JOIN stk_articulos ON movimientos_pendientes.libro_id = stk_articulos.id AND stk_articulos.enterprise_id = movimientos_pendientes.enterprise_id
                                WHERE movimientos_pendientes.id = %s AND movimientos_pendientes.enterprise_id = %s
                             """, (id, g.user['enterprise_id']))
                             l_row = cursor.fetchone()
                             
                             if u_row and u_row[0] and l_row:
                                  app_ctx = current_app._get_current_object()
                                  ent_id = g.user['enterprise_id']
                                  u_email, u_nombre = u_row
                                  libro_data = {
                                      'titulo': l_row[0],
                                      'autor': l_row[1] or '',
                                      'isbn': l_row[2] or ''
                                  }
                                  
                                  def send_cancel_async(app, email, nombre, l_data, mot, eid):
                                      with app.app_context():
                                          email_service.enviar_cancelacion_reserva(email, nombre, l_data, mot, eid)
                                  
                                  threading.Thread(target=send_cancel_async, args=(app_ctx, u_email, u_nombre, libro_data, motivo, ent_id)).start()
                except Exception as ex_mail:
                    logger.error(f"Error enviando mail cancelación: {ex_mail}")

                flash("Reserva anulada correctamente y notificada al usuario", "success")
            else:
                flash("Reserva no encontrada", "danger")
    except Exception as e:
        flash(f"Error al anular: {e}", "danger")
    return redirect(url_for('biblioteca.prestamos'))
