
from flask import Blueprint, render_template, request, redirect, url_for, flash, g
from database import get_db_cursor, atomic_transaction
from core.decorators import login_required, permission_required
from werkzeug.security import generate_password_hash
from services.enterprise_init import initialize_enterprise_master_data
from services.validation_service import format_cuit
import datetime

ent_bp = Blueprint('enterprise', __name__, template_folder='templates')

@ent_bp.route('/sysadmin/enterprises')
@login_required
@permission_required('sysadmin')
def list_enterprises():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, codigo, nombre, estado, fecha_creacion, logo_path, cuit, domicilio, condicion_iva, ingresos_brutos, inicio_actividades, iibb_condicion, afip_crt, afip_key, afip_entorno, cuenta_mailing, mailing_password FROM sys_enterprises ORDER BY fecha_creacion DESC")
        enterprises = cursor.fetchall()

        # Pre-load migration metadata to avoid AJAX Cookie issues
        cursor.execute("SELECT id, codigo, nombre FROM sys_enterprises WHERE estado = 'activo' ORDER BY nombre")
        sources = [{'id': r['id'], 'label': f"{r['id']} - {r['nombre']} ({r['codigo']})"} for r in cursor.fetchall()]

        tables = [
            {'name': 'sys_permissions', 'label': 'Permisos del Sistema (Base)', 'checked': True},
            {'name': 'stock_motivos', 'label': 'Motivos de Stock (Config)', 'checked': True},
            {'name': 'sys_roles', 'label': 'Roles Predefinidos', 'checked': False},
            {'name': 'libros', 'label': 'Catálogo de Libros (Base)', 'checked': False},
        ]
        
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'

    return render_template('sysadmin_enterprises.html', 
                          enterprises=enterprises, 
                          migration_metadata={'sources': sources, 'tables': tables},
                          now_t=int(datetime.datetime.now().timestamp()),
                          is_super=is_super)

@ent_bp.route('/sysadmin/enterprises/create', methods=['GET', 'POST'])
@atomic_transaction('enterprise', severity=9, impact_category='Compliance')
def create_enterprise_public():
    if request.method == 'POST':
        ent_id = request.form.get('id').strip()
        nombre = request.form.get('nombre').strip()
        admin_user = request.form.get('admin_user').strip()
        admin_pass = request.form.get('admin_pass')

        # AFIP Config
        afip_cuit = request.form.get('afip_cuit', '').strip()
        afip_crt = request.form.get('afip_crt', '').strip()
        afip_key = request.form.get('afip_key', '').strip()
        afip_entorno = request.form.get('afip_entorno', 'testing')

        if not ent_id or not nombre or not admin_user or not admin_pass:
            flash("Todos los campos son requeridos", "danger")
            return render_template('create_enterprise.html')
            
        try:
            with get_db_cursor(dictionary=True) as cursor:
                # 1. Create enterprise
                afip_cuit = format_cuit(afip_cuit)
                cursor.execute("""
                    INSERT INTO sys_enterprises (codigo, nombre, cuit, afip_crt, afip_key, afip_entorno) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (ent_id, nombre, afip_cuit, afip_crt, afip_key, afip_entorno))
                new_ent_id = cursor.lastrowid
                
                # 2. Initialize Master Data using the robust utility (Standard for all enterprises)
                enable_sod = request.form.get('enable_sod') == '1'
                init_results = initialize_enterprise_master_data(new_ent_id, init_sod=enable_sod, existing_cursor=cursor)
                if init_results.get('errors'):
                    flash(f"Empresa creada con algunas advertencias en datos maestros: {init_results['errors'][0]}", "warning")
                
                # 3. Handle logo upload
                if 'logo' in request.files:
                    logo_file = request.files['logo']
                    if logo_file and logo_file.filename:
                        # Validate file type
                        allowed_extensions = {'png', 'jpg', 'jpeg'}
                        filename = logo_file.filename.lower()
                        if '.' in filename and filename.rsplit('.', 1)[1] in allowed_extensions:
                            # Save to database as BLOB
                            logo_data = logo_file.read()
                            mime_type = logo_file.content_type or 'image/jpeg'
                            
                            cursor.execute("""
                                INSERT INTO sys_enterprise_logos (enterprise_id, logo_data, mime_type, is_active)
                                VALUES (%s, %s, %s, 1)
                            """, (new_ent_id, logo_data, mime_type))
                            logo_id = cursor.lastrowid
                            
                            logo_path = f"/sysadmin/enterprises/logo/raw/{logo_id}"
                            cursor.execute("UPDATE sys_enterprises SET logo_path = %s WHERE id = %s", (logo_path, new_ent_id))

                # 4. Create admin user
                # We need to find the correct Admin role created by initialize_enterprise_master_data
                cursor.execute("SELECT id FROM sys_roles WHERE enterprise_id = %s AND (name = 'Administrador' OR name = 'admin') LIMIT 1", (new_ent_id,))
                role_row = cursor.fetchone()
                role_id = role_row['id'] if role_row else 1 # Fallback if init failed
                
                h = generate_password_hash(admin_pass)
                cursor.execute("""
                    INSERT INTO sys_users (enterprise_id, username, password_hash, role_id, email) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (new_ent_id, admin_user, h, role_id, f"admin@{ent_id}.com"))

            flash(f"Empresa {nombre} creada exitosamente. Ya puede iniciar sesión.", "success")
            return redirect(url_for('core.login'))
        except Exception as e:
            flash(f"Error: {e}", "danger")
            
    return render_template('create_enterprise.html')

@ent_bp.route('/sysadmin/enterprises/toggle_status/<int:ent_id>', methods=['POST'])
@login_required
@permission_required('sysadmin')
def toggle_enterprise_status(ent_id):
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT estado FROM sys_enterprises WHERE id = %s", (ent_id,))
            res = cursor.fetchone()
            if not res:
                flash("Empresa no encontrada", "danger")
                return redirect(url_for('enterprise.list_enterprises'))
            
            new_status = 'inactivo' if res[0] == 'activo' else 'activo'
            cursor.execute("UPDATE sys_enterprises SET estado = %s WHERE id = %s", (new_status, ent_id))
            flash(f"Estado cambiado a {new_status}", "success")
    except Exception as e:
        flash(f"Error: {e}", "danger")
    return redirect(url_for('enterprise.list_enterprises'))

@ent_bp.route('/sysadmin/enterprises/update', methods=['POST'])
@login_required
@permission_required('sysadmin')
def update_enterprise():
    ent_id = request.form.get('id')
    nombre = request.form.get('nombre')
    codigo = request.form.get('codigo')
    selected_from_history = request.form.get('selected_logo_path')
    
    # Datos fiscales
    cuit = request.form.get('cuit', '').strip()
    domicilio = request.form.get('domicilio', '').strip()
    condicion_iva = request.form.get('condicion_iva', '').strip()
    ingresos_brutos = request.form.get('ingresos_brutos', '').strip()
    iibb_condicion = request.form.get('iibb_condicion', '').strip()
    inicio_actividades_raw = request.form.get('inicio_actividades', '').strip()

    # AFIP Config
    afip_cuit = request.form.get('afip_cuit', '').strip()
    afip_crt = request.form.get('afip_crt', '').strip()
    afip_key = request.form.get('afip_key', '').strip()
    afip_entorno = request.form.get('afip_entorno', 'testing')

    # Handle AFIP file uploads
    if 'afip_crt_file' in request.files and request.files['afip_crt_file'].filename:
        afip_crt = request.files['afip_crt_file'].read().decode('utf-8', errors='ignore')
    if 'afip_key_file' in request.files and request.files['afip_key_file'].filename:
        afip_key = request.files['afip_key_file'].read().decode('utf-8', errors='ignore')

    # Si afip_cuit está presente y cuit no, o son diferentes, priorizar afip_cuit para coherencia
    if afip_cuit and not cuit:
        cuit = afip_cuit
    
    cuit = format_cuit(cuit)
    afip_cuit = format_cuit(afip_cuit)
    
    # Email Config (Only Superadmin)
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'
    cuenta_mailing = request.form.get('cuenta_mailing', '').strip() if is_super else None
    mailing_password_raw = request.form.get('mailing_password', '').strip() if is_super else None

    inicio_actividades = None
    if inicio_actividades_raw:
        try:
            inicio_actividades = datetime.datetime.strptime(inicio_actividades_raw, '%Y-%m-%d').date()
        except ValueError:
            try:
                inicio_actividades = datetime.datetime.strptime(inicio_actividades_raw, '%d/%m/%Y').date()
            except ValueError:
                inicio_actividades = None
    
    try:
        with get_db_cursor() as cursor:
            logo_path = None
            # Case 1: New file uploaded
            if 'logo' in request.files and request.files['logo'].filename:
                logo_file = request.files['logo']
                logo_data = logo_file.read()
                mime_type = logo_file.content_type or 'image/jpeg'
                
                if len(logo_data) > 2 * 1024 * 1024:
                    flash("Logo demasiado grande (max 2MB)", "warning")
                else:
                    cursor.execute("UPDATE sys_enterprise_logos SET is_active = 0 WHERE enterprise_id = %s", (ent_id,))
                    cursor.execute("""
                        INSERT INTO sys_enterprise_logos (enterprise_id, logo_data, mime_type, is_active)
                        VALUES (%s, %s, %s, 1)
                    """, (ent_id, logo_data, mime_type))
                    logo_id = cursor.lastrowid
                    logo_path = f"/sysadmin/enterprises/logo/raw/{logo_id}"
            
            # Case 2: Selected from history
            elif selected_from_history:
                try:
                    logo_id = selected_from_history.split('/')[-1]
                    cursor.execute("UPDATE sys_enterprise_logos SET is_active = 0 WHERE enterprise_id = %s", (ent_id,))
                    cursor.execute("UPDATE sys_enterprise_logos SET is_active = 1 WHERE id = %s AND enterprise_id = %s", (logo_id, ent_id))
                    logo_path = selected_from_history
                except:
                    pass

            # Final Update of sys_enterprises
            set_clause = """
                nombre = %s, codigo = %s, cuit = %s, domicilio = %s, condicion_iva = %s, 
                ingresos_brutos = %s, iibb_condicion = %s, inicio_actividades = %s,
                afip_crt = %s, afip_key = %s, afip_entorno = %s
            """
            params = [nombre, codigo, cuit, domicilio, condicion_iva, ingresos_brutos, iibb_condicion, inicio_actividades, afip_crt, afip_key, afip_entorno]

            if logo_path:
                set_clause += ", logo_path = %s"
                params.append(logo_path)
            
            if is_super:
                set_clause += ", cuenta_mailing = %s"
                params.append(cuenta_mailing)
                if mailing_password_raw:
                    # Encriptar la clave antes de guardar
                    from cryptography.fernet import Fernet
                    import os
                    key_path = os.path.join(os.path.dirname(__file__), '../../multiMCP', 'secret.key')
                    if os.path.exists(key_path):
                        with open(key_path, 'rb') as key_file:
                            key = key_file.read()
                            cipher_suite = Fernet(key)
                            encrypted_pwd = cipher_suite.encrypt(mailing_password_raw.encode("utf-8")).decode("utf-8")
                            set_clause += ", mailing_password = %s"
                            params.append(encrypted_pwd)
            
            params.append(ent_id)
            cursor.execute(f"UPDATE sys_enterprises SET {set_clause} WHERE id = %s", tuple(params))
            
        flash(f"Datos de {nombre} actualizados", "success")
    except Exception as e:
        flash(f"Error al actualizar empresa: {e}", "danger")
    return redirect(url_for('enterprise.list_enterprises'))

@ent_bp.route('/sysadmin/enterprises/migration-metadata', methods=['POST'])
# @login_required
def get_migration_metadata():
    print(f"DEBUG: Migration metadata requested. User: {g.user}")
    # DIAGNOSTIC MODE: Check user manually
    if g.user is None:
        return {'error': 'Sesión no detectada por el servidor (Cookies missing)'}, 401

    # Manual Permission Check
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'
    if not is_super and 'sysadmin' not in g.permissions:
        return {'error': f"Acceso Denegado. Usuario: {g.user.get('username')}"}, 403

    try:
        with get_db_cursor() as cursor:
            # 1. Get potential source enterprises
            cursor.execute("SELECT id, codigo, nombre FROM sys_enterprises WHERE estado = 'activo' ORDER BY nombre")
            rows = cursor.fetchall()
            print(f"DEBUG: Found {len(rows)} enterprises")
            sources = [{'id': r[0], 'label': f"{r[0]} - {r[2]} ({r[1]})"} for r in rows]
            
            # 2. Define migratable tables (configuration/catalogs)
            tables = [
                {'name': 'sys_permissions', 'label': 'Permisos del Sistema (Base)', 'checked': True},
                {'name': 'stock_motivos', 'label': 'Motivos de Stock (Config)', 'checked': True},
                {'name': 'sys_roles', 'label': 'Roles Predefinidos', 'checked': False},
                {'name': 'libros', 'label': 'Catálogo de Libros (Base)', 'checked': False},
            ]
            
        return {'sources': sources, 'tables': tables}
    except Exception as e:
        print(f"DEBUG ERROR: {e}")
        return {'error': str(e)}, 500

@ent_bp.route('/sysadmin/enterprises/migrate-data', methods=['POST'])
@login_required
@permission_required('sysadmin')
@atomic_transaction('enterprise', severity=7, impact_category='Operational')
def migrate_data():
    target_id = request.form.get('target_id')
    source_id = request.form.get('source_id')
    selected_tables = request.form.getlist('tables')
    
    if not target_id or not source_id:
        flash("Faltan identificadores de empresa origen o destino", "danger")
        return redirect(url_for('enterprise.list_enterprises'))

    if not selected_tables:
        flash("No se seleccionaron tablas para migrar", "warning")
        return redirect(url_for('enterprise.list_enterprises'))
        
    final_msg = []

    try:
        with get_db_cursor() as cursor:
            for table in selected_tables:
                # Security whitelist
                if table not in ['sys_permissions', 'stock_motivos', 'sys_roles', 'libros']:
                    continue
                
                # Dynamic Column Fetch logic
                cursor.execute(f"SHOW COLUMNS FROM `{table}`")
                columns = [r[0] for r in cursor.fetchall()]
                
                # Build column lists - exclude ID (auto) and enterprise_id (param)
                cols_to_copy = [c for c in columns if c.lower() not in ('id', 'enterprise_id')]
                
                if not cols_to_copy: continue

                cols_str = ", ".join(cols_to_copy)
                placeholders = ", ".join(["%s"] * len(cols_to_copy)) # Not used in INSERT SELECT
                
                # The Query: INSERT INTO table (ent_id, col1...) SELECT new_ent_id, col1... FROM table WHERE ent_id = old_ent_id
                # Fix: Need to pass target_id as literal or param to SELECT
                
                # Construct SELECT part
                select_cols = ", ".join(cols_to_copy)
                
                query = f"""
                    INSERT IGNORE INTO `{table}` 
                    (enterprise_id, {cols_str}) 
                    SELECT %s, {select_cols} 
                    FROM `{table}` 
                    WHERE enterprise_id = %s
                """
                
                cursor.execute(query, (target_id, source_id))
                rows = cursor.rowcount
                final_msg.append(f"{table}: {rows}")
            
            flash(f"Migración completada: {', '.join(final_msg)}", "success")
            
    except Exception as e:
        flash(f"Error en migración: {e}", "danger")
        
    return redirect(url_for('enterprise.list_enterprises'))

@ent_bp.route('/sysadmin/enterprises/fiscal/<int:ent_id>')
@login_required
def get_enterprise_fiscal(ent_id):
    # Manual Permission Check
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'
    if not is_super and 'sysadmin' not in g.permissions:
        # Check if user belongs to this enterprise and has right permission
        if g.user['enterprise_id'] != ent_id:
            return {'error': 'Acceso denegado'}, 403
    
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM sys_enterprises_fiscal WHERE enterprise_id = %s AND activo = 1", (ent_id,))
        agents = cursor.fetchall()
        # Format dates for JSON
        for a in agents:
            if a['fecha_notificacion']:
                a['fecha_notificacion'] = a['fecha_notificacion'].isoformat()
                
    return {'agents': agents}

@ent_bp.route('/sysadmin/enterprises/fiscal/save', methods=['POST'])
@login_required
def save_enterprise_fiscal():
    data = request.json
    ent_id = data.get('enterprise_id')
    
    # Permission check
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'
    if not is_super and 'sysadmin' not in g.permissions:
        if g.user['enterprise_id'] != ent_id:
            return {'error': 'Acceso denegado'}, 403

    jurisdiccion = data.get('jurisdiccion')
    tipo = data.get('tipo', 'AMBOS')
    nro_notificacion = data.get('nro_notificacion')
    fecha_notificacion = data.get('fecha_notificacion') or None
    agent_id = data.get('id')

    with get_db_cursor() as cursor:
        if agent_id:
            cursor.execute("""
                UPDATE sys_enterprises_fiscal 
                SET jurisdiccion = %s, tipo = %s, fecha_notificacion = %s, nro_notificacion = %s
                WHERE id = %s AND enterprise_id = %s
            """, (jurisdiccion, tipo, fecha_notificacion, nro_notificacion, agent_id, ent_id))
        else:
            cursor.execute("""
                INSERT INTO sys_enterprises_fiscal (enterprise_id, jurisdiccion, tipo, fecha_notificacion, nro_notificacion)
                VALUES (%s, %s, %s, %s, %s)
            """, (ent_id, jurisdiccion, tipo, fecha_notificacion, nro_notificacion))
            
    return {'success': True}

@ent_bp.route('/sysadmin/enterprises/fiscal/delete/<int:agent_id>', methods=['POST'])
@login_required
def delete_enterprise_fiscal(agent_id):
    with get_db_cursor(dictionary=True) as cursor:
        # Get ent_id for permission check
        cursor.execute("SELECT enterprise_id FROM sys_enterprises_fiscal WHERE id = %s", (agent_id,))
        row = cursor.fetchone()
        if not row: return {'error': 'Not found'}, 404
        
        ent_id = row['enterprise_id']
        is_super = str(g.user.get('username', '')).lower() == 'superadmin'
        if not is_super and 'sysadmin' not in g.permissions:
            if g.user['enterprise_id'] != ent_id:
                return {'error': 'Acceso denegado'}, 403
                
        cursor.execute("UPDATE sys_enterprises_fiscal SET activo = 0 WHERE id = %s", (agent_id,))
        
    return {'success': True}

@ent_bp.route('/sysadmin/enterprises/logos/history/<int:ent_id>')
@login_required
@permission_required('sysadmin')
def get_logo_history(ent_id):
    with get_db_cursor() as cursor:
        cursor.execute("""
            SELECT id, mime_type, created_at, is_active 
            FROM sys_enterprise_logos 
            WHERE enterprise_id = %s 
            ORDER BY created_at DESC
        """, (ent_id,))
        rows = cursor.fetchall()
        
    history = []
    for r in rows:
        history.append({
            'id': r[0],
            'path': f"/sysadmin/enterprises/logo/raw/{r[0]}",
            'mime': r[1],
            'created_at': str(r[2]),
            'is_active': bool(r[3])
        })
    return {'history': history}

@ent_bp.route('/sysadmin/enterprises/logo/raw/<int:logo_id>')
def get_logo_raw(logo_id):
    from flask import make_response
    with get_db_cursor() as cursor:
        cursor.execute("SELECT logo_data, mime_type FROM sys_enterprise_logos WHERE id = %s", (logo_id,))
        row = cursor.fetchone()
        if not row:
            return "Logo no encontrado", 404
        
        response = make_response(row[0])
        response.headers.set('Content-Type', row[1])
        # Cache for 1 day
        response.headers.set('Cache-Control', 'public, max-age=86400')
        return response

@ent_bp.route('/sysadmin/saas-owner')
@login_required
@permission_required('sysadmin')
def saas_owner_master():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM sys_enterprises WHERE is_saas_owner = 1 LIMIT 1")
        saas_owner = cursor.fetchone()
    
    if not saas_owner:
        flash("No se encontró la configuración de SaaS Owner.", "warning")
        return redirect(url_for('enterprise.list_enterprises'))
        
    return render_template('saas_owner_master.html', saas_owner=saas_owner)

@ent_bp.route('/sysadmin/saas-owner/save', methods=['POST'])
@login_required
@permission_required('sysadmin')
def saas_owner_save():
    email_recuperacion = request.form.get('email', '').strip()
    nombre = request.form.get('nombre', '').strip()
    telefono = request.form.get('telefono', '').strip()
    website = request.form.get('website', '').strip()
    lema = request.form.get('lema', '').strip()
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                UPDATE sys_enterprises 
                SET email = %s, nombre = %s, telefono = %s, website = %s, lema = %s
                WHERE is_saas_owner = 1
            """, (email_recuperacion, nombre, telefono, website, lema))
        flash("Maestro SaaS Owner actualizado.", "success")
    except Exception as e:
        flash(f"Error al guardar: {e}", "danger")
        
    return redirect(url_for('enterprise.saas_owner_master'))

@ent_bp.route('/sysadmin/sod-matrix')
@login_required
@permission_required('sysadmin')
def sod_matrix():
    # Strict SuperAdmin check
    if str(g.user.get('username', '')).lower() != 'superadmin':
        flash("Acceso restringido a SuperAdmin", "danger")
        return redirect(url_for('enterprise.list_enterprises'))
        
    from services.sod_service import ROLES_SOD
    
    # 1. Collect unique permissions from Standard
    all_perm_codes = set()
    for r in ROLES_SOD.values():
        all_perm_codes.update(r['permisos'])
        
    matrix_cols = {}
    
    # 2. Fetch Metadata for Columns
    if all_perm_codes:
        with get_db_cursor() as cursor:
            placeholders = ','.join(['%s'] * len(all_perm_codes))
            sql = f"""
                SELECT code, MIN(description) as description, MIN(category) as category 
                FROM sys_permissions 
                WHERE code IN ({placeholders}) 
                GROUP BY code
                ORDER BY category, code
            """
            cursor.execute(sql, tuple(all_perm_codes))
            rows = cursor.fetchall()
            
            for r in rows:
                cat = r[2] or 'General'
                if cat not in matrix_cols: matrix_cols[cat] = []
                matrix_cols[cat].append({'code': r[0], 'desc': r[1]})

    # 3. Fetch ACTUAL Permissions for Audit
    actual_permissions = {}  # { 'ROLE_NAME': set(['perm_code', ...]) }
    with get_db_cursor() as cursor:
        sql_audit = """
            SELECT r.name, p.code 
            FROM sys_role_permissions rp
            JOIN sys_roles r ON rp.role_id = r.id
            JOIN sys_permissions p ON rp.permission_id = p.id
            WHERE r.enterprise_id = %s
        """
        cursor.execute(sql_audit, (g.user['enterprise_id'],))
        audit_rows = cursor.fetchall()
        for r_name, p_code in audit_rows:
            if r_name not in actual_permissions:
                actual_permissions[r_name] = set()
            actual_permissions[r_name].add(p_code)
                
    return render_template('sysadmin_sod_matrix.html', 
                          role_rows=ROLES_SOD, 
                          matrix_cols=matrix_cols,
                          actual_permissions=actual_permissions)

