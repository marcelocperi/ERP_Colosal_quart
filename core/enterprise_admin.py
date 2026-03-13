
from quart import Blueprint, render_template, request, redirect, url_for, flash, g
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
async def list_enterprises():
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT id, codigo, nombre, estado, fecha_creacion, logo_path, cuit, domicilio, condicion_iva, ingresos_brutos, inicio_actividades, iibb_condicion, afip_crt, afip_key, afip_entorno, cuenta_mailing, mailing_password FROM sys_enterprises ORDER BY fecha_creacion DESC")
        enterprises = await cursor.fetchall()

        # Pre-load migration metadata to avoid AJAX Cookie issues
        await cursor.execute("SELECT id, codigo, nombre FROM sys_enterprises WHERE estado = 'activo' ORDER BY nombre")
        sources = [{'id': r['id'], 'label': f"{r['id']} - {r['nombre']} ({r['codigo']})"} for r in await cursor.fetchall()]

        tables = [
            {'name': 'sys_permissions', 'label': 'Permisos del Sistema (Base)', 'checked': True},
            {'name': 'stock_motivos', 'label': 'Motivos de Stock (Config)', 'checked': True},
            {'name': 'sys_roles', 'label': 'Roles Predefinidos', 'checked': False},
            {'name': 'libros', 'label': 'Catálogo de Libros (Base)', 'checked': False},
        ]
        
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'

    return await render_template('sysadmin_enterprises.html', 
                          enterprises=enterprises, 
                          migration_metadata={'sources': sources, 'tables': tables},
                          now_t=int(datetime.datetime.now().timestamp()),
                          is_super=is_super)

@ent_bp.route('/sysadmin/enterprises/create', methods=['GET', 'POST'])
@atomic_transaction('enterprise', severity=9, impact_category='Compliance')
async def create_enterprise_public():
    if request.method == 'POST':
        ent_id = (await request.form).get('id').strip()
        nombre = (await request.form).get('nombre').strip()
        admin_user = (await request.form).get('admin_user').strip()
        admin_pass = (await request.form).get('admin_pass')

        # AFIP Config
        afip_cuit = (await request.form).get('afip_cuit', '').strip()
        afip_crt = (await request.form).get('afip_crt', '').strip()
        afip_key = (await request.form).get('afip_key', '').strip()
        afip_entorno = (await request.form).get('afip_entorno', 'testing')

        if not ent_id or not nombre or not admin_user or not admin_pass:
            await flash("Todos los campos son requeridos", "danger")
            return await render_template('create_enterprise.html')
            
        try:
            async with get_db_cursor(dictionary=True) as cursor:
                # 1. Create enterprise
                afip_cuit = format_cuit(afip_cuit)
                await cursor.execute("""
                    INSERT INTO sys_enterprises (codigo, nombre, cuit, afip_crt, afip_key, afip_entorno) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (ent_id, nombre, afip_cuit, afip_crt, afip_key, afip_entorno))
                new_ent_id = cursor.lastrowid
                
                # 2. Initialize Master Data using the robust utility (Standard for all enterprises)
                enable_sod = (await request.form).get('enable_sod') == '1'
                init_results = await initialize_enterprise_master_data(new_ent_id, init_sod=enable_sod, existing_cursor=cursor)
                if init_results.get('errors'):
                    await flash(f"Empresa creada con algunas advertencias en datos maestros: {init_results['errors'][0]}", "warning")
                
                # 3. Handle logo upload
                if 'logo' in (await request.files):
                    logo_file = (await request.files)['logo']
                    if logo_file and logo_file.filename:
                        # Validate file type
                        allowed_extensions = {'png', 'jpg', 'jpeg'}
                        filename = logo_file.filename.lower()
                        if '.' in filename and filename.rsplit('.', 1)[1] in allowed_extensions:
                            # Save to database as BLOB
                            logo_data = await logo_file.read()
                            mime_type = logo_file.content_type or 'image/jpeg'
                            
                            await cursor.execute("""
                                INSERT INTO sys_enterprise_logos (enterprise_id, logo_data, mime_type, is_active)
                                VALUES (%s, %s, %s, 1)
                            """, (new_ent_id, logo_data, mime_type))
                            logo_id = cursor.lastrowid
                            
                            logo_path = f"/sysadmin/enterprises/logo/raw/{logo_id}"
                            await cursor.execute("UPDATE sys_enterprises SET logo_path = %s WHERE id = %s", (logo_path, new_ent_id))

                # 4. Create admin user
                # We need to find the correct Admin role created by initialize_enterprise_master_data
                await cursor.execute("SELECT id FROM sys_roles WHERE enterprise_id = %s AND (name = 'Administrador' OR name = 'admin') LIMIT 1", (new_ent_id,))
                role_row = await cursor.fetchone()
                role_id = role_row['id'] if role_row else 1 # Fallback if init failed
                
                h = generate_password_hash(admin_pass)
                await cursor.execute("""
                    INSERT INTO sys_users (enterprise_id, username, password_hash, role_id, email) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (new_ent_id, admin_user, h, role_id, f"admin@{ent_id}.com"))

            await flash(f"Empresa {nombre} creada exitosamente. Ya puede iniciar sesión.", "success")
            return redirect(url_for('core.login'))
        except Exception as e:
            await flash(f"Error: {e}", "danger")
            
    return await render_template('create_enterprise.html')

@ent_bp.route('/sysadmin/enterprises/toggle_status/<int:ent_id>', methods=['POST'])
@login_required
@permission_required('sysadmin')
async def toggle_enterprise_status(ent_id):
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("SELECT estado FROM sys_enterprises WHERE id = %s", (ent_id,))
            res = await cursor.fetchone()
            if not res:
                await flash("Empresa no encontrada", "danger")
                return redirect(url_for('enterprise.list_enterprises'))
            
            new_status = 'inactivo' if res[0] == 'activo' else 'activo'
            await cursor.execute("UPDATE sys_enterprises SET estado = %s WHERE id = %s", (new_status, ent_id))
            await flash(f"Estado cambiado a {new_status}", "success")
    except Exception as e:
        await flash(f"Error: {e}", "danger")
    return redirect(url_for('enterprise.list_enterprises'))

@ent_bp.route('/sysadmin/enterprises/update', methods=['POST'])
@login_required
@permission_required('sysadmin')
async def update_enterprise():
    ent_id = (await request.form).get('id')
    nombre = (await request.form).get('nombre')
    codigo = (await request.form).get('codigo')
    selected_from_history = (await request.form).get('selected_logo_path')
    
    # Datos fiscales
    cuit = (await request.form).get('cuit', '').strip()
    domicilio = (await request.form).get('domicilio', '').strip()
    condicion_iva = (await request.form).get('condicion_iva', '').strip()
    ingresos_brutos = (await request.form).get('ingresos_brutos', '').strip()
    iibb_condicion = (await request.form).get('iibb_condicion', '').strip()
    inicio_actividades_raw = (await request.form).get('inicio_actividades', '').strip()

    # AFIP Config
    afip_cuit = (await request.form).get('afip_cuit', '').strip()
    afip_crt = (await request.form).get('afip_crt', '').strip()
    afip_key = (await request.form).get('afip_key', '').strip()
    afip_entorno = (await request.form).get('afip_entorno', 'testing')

    # Handle AFIP file uploads
    if 'afip_crt_file' in (await request.files) and (await request.files)['afip_crt_file'].filename:
        afip_crt = (await (await request.files)['afip_crt_file'].read()).decode('utf-8', errors='ignore')
    if 'afip_key_file' in (await request.files) and (await request.files)['afip_key_file'].filename:
        afip_key = (await (await request.files)['afip_key_file'].read()).decode('utf-8', errors='ignore')

    # Si afip_cuit está presente y cuit no, o son diferentes, priorizar afip_cuit para coherencia
    if afip_cuit and not cuit:
        cuit = afip_cuit
    
    cuit = format_cuit(cuit)
    afip_cuit = format_cuit(afip_cuit)
    
    # Email Config (Only Superadmin)
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'
    cuenta_mailing = (await request.form).get('cuenta_mailing', '').strip() if is_super else None
    mailing_password_raw = (await request.form).get('mailing_password', '').strip() if is_super else None

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
        async with get_db_cursor() as cursor:
            logo_path = None
            # Case 1: New file uploaded
            if 'logo' in (await request.files) and (await request.files)['logo'].filename:
                logo_file = (await request.files)['logo']
                logo_data = await logo_file.read()
                mime_type = logo_file.content_type or 'image/jpeg'
                
                if len(logo_data) > 2 * 1024 * 1024:
                    await flash("Logo demasiado grande (max 2MB)", "warning")
                else:
                    await cursor.execute("UPDATE sys_enterprise_logos SET is_active = 0 WHERE enterprise_id = %s", (ent_id,))
                    await cursor.execute("""
                        INSERT INTO sys_enterprise_logos (enterprise_id, logo_data, mime_type, is_active)
                        VALUES (%s, %s, %s, 1)
                    """, (ent_id, logo_data, mime_type))
                    logo_id = cursor.lastrowid
                    logo_path = f"/sysadmin/enterprises/logo/raw/{logo_id}"
            
            # Case 2: Selected from history
            elif selected_from_history:
                try:
                    logo_id = selected_from_history.split('/')[-1]
                    await cursor.execute("UPDATE sys_enterprise_logos SET is_active = 0 WHERE enterprise_id = %s", (ent_id,))
                    await cursor.execute("UPDATE sys_enterprise_logos SET is_active = 1 WHERE id = %s AND enterprise_id = %s", (logo_id, ent_id))
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
                            key = await key_file.read()
                            cipher_suite = Fernet(key)
                            encrypted_pwd = cipher_suite.encrypt(mailing_password_raw.encode("utf-8")).decode("utf-8")
                            set_clause += ", mailing_password = %s"
                            params.append(encrypted_pwd)
            
            params.append(ent_id)
            await cursor.execute(f"UPDATE sys_enterprises SET {set_clause} WHERE id = %s", tuple(params))
            
        await flash(f"Datos de {nombre} actualizados", "success")
    except Exception as e:
        await flash(f"Error al actualizar empresa: {e}", "danger")
    return redirect(url_for('enterprise.list_enterprises'))

@ent_bp.route('/sysadmin/enterprises/migration-metadata', methods=['POST'])
# @login_required
async def get_migration_metadata():
    print(f"DEBUG: Migration metadata requested. User: {g.user}")
    # DIAGNOSTIC MODE: Check user manually
    if g.user is None:
        return {'error': 'Sesión no detectada por el servidor (Cookies missing)'}, 401

    # Manual Permission Check
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'
    if not is_super and 'sysadmin' not in g.permissions:
        return {'error': f"Acceso Denegado. Usuario: {g.user.get('username')}"}, 403

    try:
        async with get_db_cursor() as cursor:
            # 1. Get potential source enterprises
            await cursor.execute("SELECT id, codigo, nombre FROM sys_enterprises WHERE estado = 'activo' ORDER BY nombre")
            rows = await cursor.fetchall()
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
async def migrate_data():
    target_id = (await request.form).get('target_id')
    source_id = (await request.form).get('source_id')
    selected_tables = (await request.form).getlist('tables')
    
    if not target_id or not source_id:
        await flash("Faltan identificadores de empresa origen o destino", "danger")
        return redirect(url_for('enterprise.list_enterprises'))

    if not selected_tables:
        await flash("No se seleccionaron tablas para migrar", "warning")
        return redirect(url_for('enterprise.list_enterprises'))
        
    final_msg = []

    try:
        async with get_db_cursor() as cursor:
            for table in selected_tables:
                # Security whitelist
                if table not in ['sys_permissions', 'stock_motivos', 'sys_roles', 'libros']:
                    continue
                
                # Dynamic Column Fetch logic
                await cursor.execute(f"SHOW COLUMNS FROM `{table}`")
                columns = [r[0] for r in await cursor.fetchall()]
                
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
                
                await cursor.execute(query, (target_id, source_id))
                rows = cursor.rowcount
                final_msg.append(f"{table}: {rows}")
            
            await flash(f"Migración completada: {', '.join(final_msg)}", "success")
            
    except Exception as e:
        await flash(f"Error en migración: {e}", "danger")
        
    return redirect(url_for('enterprise.list_enterprises'))

@ent_bp.route('/sysadmin/enterprises/fiscal/<int:ent_id>')
@login_required
async def get_enterprise_fiscal(ent_id):
    # Manual Permission Check
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'
    if not is_super and 'sysadmin' not in g.permissions:
        # Check if user belongs to this enterprise and has right permission
        if g.user['enterprise_id'] != ent_id:
            return {'error': 'Acceso denegado'}, 403
    
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM sys_enterprises_fiscal WHERE enterprise_id = %s AND activo = 1", (ent_id,))
        agents = await cursor.fetchall()
        # Format dates for JSON
        for a in agents:
            if a['fecha_notificacion']:
                a['fecha_notificacion'] = a['fecha_notificacion'].isoformat()
                
    return {'agents': agents}

@ent_bp.route('/sysadmin/enterprises/fiscal/save', methods=['POST'])
@login_required
async def save_enterprise_fiscal():
    data = (await request.json)
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

    async with get_db_cursor() as cursor:
        if agent_id:
            await cursor.execute("""
                UPDATE sys_enterprises_fiscal 
                SET jurisdiccion = %s, tipo = %s, fecha_notificacion = %s, nro_notificacion = %s
                WHERE id = %s AND enterprise_id = %s
            """, (jurisdiccion, tipo, fecha_notificacion, nro_notificacion, agent_id, ent_id))
        else:
            await cursor.execute("""
                INSERT INTO sys_enterprises_fiscal (enterprise_id, jurisdiccion, tipo, fecha_notificacion, nro_notificacion)
                VALUES (%s, %s, %s, %s, %s)
            """, (ent_id, jurisdiccion, tipo, fecha_notificacion, nro_notificacion))
            
    return {'success': True}

@ent_bp.route('/sysadmin/enterprises/fiscal/delete/<int:agent_id>', methods=['POST'])
@login_required
async def delete_enterprise_fiscal(agent_id):
    async with get_db_cursor(dictionary=True) as cursor:
        # Get ent_id for permission check
        await cursor.execute("SELECT enterprise_id FROM sys_enterprises_fiscal WHERE id = %s", (agent_id,))
        row = await cursor.fetchone()
        if not row: return {'error': 'Not found'}, 404
        
        ent_id = row['enterprise_id']
        is_super = str(g.user.get('username', '')).lower() == 'superadmin'
        if not is_super and 'sysadmin' not in g.permissions:
            if g.user['enterprise_id'] != ent_id:
                return {'error': 'Acceso denegado'}, 403
                
        await cursor.execute("UPDATE sys_enterprises_fiscal SET activo = 0 WHERE id = %s", (agent_id,))
        
    return {'success': True}

@ent_bp.route('/sysadmin/enterprises/logos/history/<int:ent_id>')
@login_required
@permission_required('sysadmin')
async def get_logo_history(ent_id):
    async with get_db_cursor() as cursor:
        await cursor.execute("""
            SELECT id, mime_type, created_at, is_active 
            FROM sys_enterprise_logos 
            WHERE enterprise_id = %s 
            ORDER BY created_at DESC
        """, (ent_id,))
        rows = await cursor.fetchall()
        
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
async def get_logo_raw(logo_id):
    from quart import make_response
    async with get_db_cursor() as cursor:
        await cursor.execute("SELECT logo_data, mime_type FROM sys_enterprise_logos WHERE id = %s", (logo_id,))
        row = await cursor.fetchone()
        if not row:
            return "Logo no encontrado", 404
        
        response = await make_response(row[0])
        response.headers.set('Content-Type', row[1])
        # Cache for 1 day
        response.headers.set('Cache-Control', 'public, max-age=86400')
        return response

@ent_bp.route('/sysadmin/saas-owner')
@login_required
@permission_required('sysadmin')
async def saas_owner_master():
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM sys_enterprises WHERE is_saas_owner = 1 LIMIT 1")
        saas_owner = await cursor.fetchone()
    
    if not saas_owner:
        await flash("No se encontró la configuración de SaaS Owner.", "warning")
        return redirect(url_for('enterprise.list_enterprises'))
        
    return await render_template('saas_owner_master.html', saas_owner=saas_owner)

@ent_bp.route('/sysadmin/saas-owner/save', methods=['POST'])
@login_required
@permission_required('sysadmin')
async def saas_owner_save():
    email_recuperacion = (await request.form).get('email', '').strip()
    nombre = (await request.form).get('nombre', '').strip()
    telefono = (await request.form).get('telefono', '').strip()
    website = (await request.form).get('website', '').strip()
    lema = (await request.form).get('lema', '').strip()
    
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("""
                UPDATE sys_enterprises 
                SET email = %s, nombre = %s, telefono = %s, website = %s, lema = %s
                WHERE is_saas_owner = 1
            """, (email_recuperacion, nombre, telefono, website, lema))
        await flash("Maestro SaaS Owner actualizado.", "success")
    except Exception as e:
        await flash(f"Error al guardar: {e}", "danger")
        
    return redirect(url_for('enterprise.saas_owner_master'))

@ent_bp.route('/sysadmin/sod-matrix')
@login_required
@permission_required('sysadmin')
async def sod_matrix():
    # Strict SuperAdmin check
    if str(g.user.get('username', '')).lower() != 'superadmin':
        await flash("Acceso restringido a SuperAdmin", "danger")
        return redirect(url_for('enterprise.list_enterprises'))
        
    from services.sod_service import ROLES_SOD
    
    # 1. Collect unique permissions from Standard
    all_perm_codes = set()
    for r in ROLES_SOD.values():
        await all_perm_codes.update(r['permisos'])
        
    matrix_cols = {}
    
    # 2. Fetch Metadata for Columns
    if all_perm_codes:
        async with get_db_cursor() as cursor:
            placeholders = ','.join(['%s'] * len(all_perm_codes))
            sql = f"""
                SELECT code, MIN(description) as description, MIN(category) as category 
                FROM sys_permissions 
                WHERE code IN ({placeholders}) 
                GROUP BY code
                ORDER BY category, code
            """
            await cursor.execute(sql, tuple(all_perm_codes))
            rows = await cursor.fetchall()
            
            for r in rows:
                cat = r[2] or 'General'
                if cat not in matrix_cols: matrix_cols[cat] = []
                matrix_cols[cat].append({'code': r[0], 'desc': r[1]})

    # 3. Fetch ACTUAL Permissions for Audit
    actual_permissions = {}  # { 'ROLE_NAME': set(['perm_code', ...]) }
    async with get_db_cursor() as cursor:
        sql_audit = """
            SELECT r.name, p.code 
            FROM sys_role_permissions rp
            JOIN sys_roles r ON rp.role_id = r.id
            JOIN sys_permissions p ON rp.permission_id = p.id
            WHERE r.enterprise_id = %s
        """
        await cursor.execute(sql_audit, (g.user['enterprise_id'],))
        audit_rows = await cursor.fetchall()
        for r_name, p_code in audit_rows:
            if r_name not in actual_permissions:
                actual_permissions[r_name] = set()
            actual_permissions[r_name].add(p_code)
                
    return await render_template('sysadmin_sod_matrix.html', 
                          role_rows=ROLES_SOD, 
                          matrix_cols=matrix_cols,
                          actual_permissions=actual_permissions)

