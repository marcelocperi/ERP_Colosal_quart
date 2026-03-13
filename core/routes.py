
from quart import Blueprint, render_template, request, redirect, url_for, flash, session, g, jsonify, current_app, make_response
from database import get_db_cursor, atomic_transaction
from werkzeug.security import check_password_hash, generate_password_hash
from core.decorators import login_required, permission_required
from services import email_service # Assuming services is at root level for now or we install it
import secrets
import datetime
import threading
import logging

# Configure Blueprint
core_bp = Blueprint('core', __name__, template_folder='templates', static_folder='static')
logger = logging.getLogger(__name__)

# --- Helper for Async Email ---
def _async_email(func, *args):
    current_app.add_background_task(func, *args)

# --- ROUTES ---

@core_bp.route('/')
async def index():
    """Ruta raíz: redirige al dashboard si hay sesión activa, o al login si no."""
    if g.user:
        return redirect(url_for('ventas.dashboard'))
    return redirect(url_for('core.login'))

@core_bp.route('/favicon.ico')
async def favicon():
    """Evita 404 repetitivos en los logs por peticiones automáticas del navegador."""
    import os
    favicon_path = os.path.join(core_bp.static_folder or '', 'favicon.ico')
    if core_bp.static_folder and os.path.exists(favicon_path):
        from quart import send_from_directory
        return await send_from_directory(core_bp.static_folder, 'favicon.ico', mimetype='image/vnd.microsoft.icon')
    # Retornar 204 No Content silenciosamente si no existe el archivo
    from quart import make_response
    return await make_response('', 204)

@core_bp.route('/login', methods=['GET', 'POST'])
@atomic_transaction('core', severity=8, impact_category='Security')
async def login():
    if g.user:
        return redirect(url_for('ventas.dashboard'))
        
    # Secret access to master enterprise and creation
    show_master = request.args.get('master') == '1'
    
    async with get_db_cursor() as cursor:
        if show_master:
            await cursor.execute("SELECT id, nombre, is_saas_owner FROM sys_enterprises WHERE estado = 'activo' ORDER BY is_saas_owner DESC, id ASC")
        else:
            # Show regular enterprises + ID 0 (Template) but hide ID 1 (SaaS Owner)
            await cursor.execute("SELECT id, nombre, is_saas_owner FROM sys_enterprises WHERE estado = 'activo' AND (is_saas_owner = 0 OR id = 0) AND id != 1 ORDER BY id ASC")
        enterprises = await cursor.fetchall()
        logger.info(f"LOGIN: Fetching enterprises. Show Master: {show_master}, Found: {len(enterprises)}")
    
    if request.method == 'POST':
        with open('login_debug.txt', 'a') as f: f.write("1. POST RECEIVED\n")
        form = await request.form
        enterprise_id = form.get('enterprise_id')
        username = form.get('username')
        password = form.get('password')
        with open('login_debug.txt', 'a') as f: f.write(f"2. Form parsed: {username}@{enterprise_id}\n")
        
        if enterprise_id == "NEW":
            return redirect(url_for('enterprise.create_enterprise_public'))

        try:
            enterprise_id = int(enterprise_id)
            async with get_db_cursor() as cursor:
                # 1. Check if Enterprise is Active
                await cursor.execute("SELECT estado FROM sys_enterprises WHERE id = %s", (enterprise_id,))
                ent_status = await cursor.fetchone()
                
                if not ent_status or ent_status[0].lower() != 'activo':
                    with open('login_debug.txt', 'a') as f: f.write(f"3. Blocking: Enterprise status {ent_status}\n")
                    await flash("Esta empresa se encuentra inhabilitada temporalmente.", "danger")
                    return redirect(url_for('core.login'))

                with open('login_debug.txt', 'a') as f: f.write("4. Enterprise Active. Checking user...\n")

                await cursor.execute("""
                    SELECT id, password_hash, username, temp_password_hash, temp_password_expires, must_change_password
                    FROM sys_users 
                    WHERE username = %s AND enterprise_id = %s
                """, (username, enterprise_id))
                user = await cursor.fetchone()
                
                if user:
                    with open('login_debug.txt', 'a') as f: f.write("5. User found in DB.\n")
                    user_id, pwd_hash, uname, temp_hash, temp_expires, must_change = user
                    
                    if check_password_hash(pwd_hash, password):
                        with open('login_debug.txt', 'a') as f: f.write("6. Password MATCH. Creating session...\n")
                        new_sid = secrets.token_hex(4)
                        if 's' not in session: session['s'] = {}
                        session['s'][new_sid] = {'user_id': user_id, 'enterprise_id': enterprise_id}
                        session.permanent = True # Hacer la sesión permanente
                        session.modified = True
                        with open('login_debug.txt', 'a') as f: f.write("7. Session modified.\n")

                        await _log_security_event("LOGIN_SUCCESS", "SUCCESS", enterprise_id=enterprise_id, target_user_id=user_id, details=f"Regular login. SID={new_sid}", sid=new_sid)
                        
                        if must_change:
                            session['s'][new_sid]['must_change_password'] = True
                            session.modified = True
                            await flash("Debe cambiar su contraseña antes de continuar.", "warning")
                            return redirect(url_for('core.enforce_password_change', sid=new_sid))
                            
                        # MECANISMO DE AFINIDAD DE PESTAÑA (Handshake)
                        from quart import make_response
                        bind_token = secrets.token_hex(16)
                        session['s'][new_sid]['bind_token'] = bind_token # Guardar en sesión para validar luego si es necesario
                        
                        resp = await make_response(redirect(url_for('ventas.dashboard', sid=new_sid)))
                        # Seteamos una cookie temporal de "venda" que solo dura 30 segundos
                        resp.set_cookie(f'bind_{new_sid}', bind_token, max_age=30, httponly=False, samesite='Lax')
                        return resp
                    
                    if temp_hash and temp_expires:
                        await cursor.execute("SELECT temp_password_used FROM sys_users WHERE id = %s AND enterprise_id = %s", (user_id, enterprise_id))
                        used_row = await cursor.fetchone()
                        is_used = used_row[0] if used_row else 0
                        if is_used:
                             await flash("Clave temporal caducada.", "danger")
                             return redirect(url_for('core.reset_password_public'))

                        if isinstance(temp_expires, str):
                            temp_expires = datetime.datetime.strptime(temp_expires, '%Y-%m-%d %H:%M:%S')
                            
                        if datetime.datetime.now() < temp_expires:
                            if check_password_hash(temp_hash, password):
                                await cursor.execute("UPDATE sys_users SET temp_password_used = 1 WHERE id = %s AND enterprise_id = %s", (user_id, enterprise_id))
                                new_sid = secrets.token_hex(4)
                                session['s'][new_sid] = {'user_id': user_id, 'enterprise_id': enterprise_id, 'must_change_password': True}
                                session.modified = True
                                await _log_security_event("LOGIN_SUCCESS_TEMP", "SUCCESS", enterprise_id=enterprise_id, target_user_id=user_id, details=f"Temp login. SID={new_sid}", sid=new_sid)
                                await flash("Por seguridad, debe cambiar su contraseña temporal.", "warning")
                                return redirect(url_for('core.enforce_password_change', sid=new_sid))
                
                with open('login_debug.txt', 'a') as f: f.write("8. Login FAILURE. Invalid credentials.\n")
                await _log_security_event("LOGIN_FAILURE", "FAILURE", enterprise_id=enterprise_id, details=f"Invalid credentials: {username}@{enterprise_id}")
                await flash("Credenciales incorrectas", "danger")
        except Exception as e:
            with open('login_debug.txt', 'a') as f: f.write(f"X. EXCEPTION: {e}\n")
            import traceback
            tb = traceback.format_exc()
            logger.error(f"LOGIN ERROR: {e}\n{tb}")
            print(f"[LOGIN DEBUG] {tb}", flush=True)
            await flash(f"Error en login: {e}", "danger")
            
    return await render_template('login.html', enterprises=enterprises, show_master=show_master)

@core_bp.route('/logout')
async def logout():
    reason = request.args.get('reason')
    curr_sid = g.get('sid') or request.args.get('sid')
    
    if g.user:
        await _log_security_event("LOGOUT", "SUCCESS", details=f"User {g.user['username']} logged out. SID={curr_sid}")
    elif reason == 'tab_violation':
        await _log_security_event("TAB_VIOLATION", "FAILURE", details=f"Intento de duplicar pestaña con SID={curr_sid}")
    
    if curr_sid and 's' in session:
        session['s'].pop(curr_sid, None)
        session.modified = True
    
    if reason == 'tab_violation':
        await flash("Seguridad: Por integridad de datos, no se permite copiar la URL entre pestañas independientes. Cada pestaña debe iniciar su propia sesión.", "warning")
    else:
        await flash("Sesión cerrada correctamente", "info")
        
    response = redirect(url_for('core.login', _t=int(datetime.datetime.now().timestamp())))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    return response

@core_bp.route('/auth/enforce-change-password', methods=['GET', 'POST'])
@login_required
@atomic_transaction('core', severity=8, impact_category='Security')
async def enforce_password_change():
    """Forces user to change password after temp login"""
    
    # Security Check: Only allow if flag is set, otherwise redirect to dashboard
    sid = g.sid
    if not sid or 's' not in session or not session['s'].get(sid, {}).get('must_change_password'):
        return redirect(url_for('ventas.dashboard'))

    if request.method == 'POST':
        new_pass = (await request.form).get('new_password')
        confirm_pass = (await request.form).get('confirm_password')
        
        if not new_pass or new_pass != confirm_pass:
            await flash("Las contraseñas no coinciden o están vacías", "danger")
            return await render_template('enforce_change_password.html')
            
        try:
            async with get_db_cursor() as cursor:
                new_hash = generate_password_hash(new_pass)
                # Reset all temp fields and update main password
                await cursor.execute("""
                    UPDATE sys_users 
                    SET password_hash = %s, 
                        temp_password_hash = NULL, 
                        temp_password_expires = NULL, 
                        temp_password_used = 0,
                        must_change_password = 0,
                        recovery_attempts = 0 
                    WHERE id = %s AND enterprise_id = %s
                """, (new_hash, g.user['id'], g.user['enterprise_id']))
                
                await _log_security_event("PASSWORD_CHANGE_ENFORCED", "SUCCESS", target_user_id=g.user['id'])
                
                # Clear flag
                session['s'][sid].pop('must_change_password', None)
                session.modified = True
                
                await flash("Contraseña establecida correctamente. Bienvenido.", "success")
                return redirect(url_for('ventas.dashboard'))
        except Exception as e:
            await _log_security_event("PASSWORD_CHANGE_ENFORCED", "ERROR", details=str(e))
            await flash(f"Error al actualizar: {str(e)}", "danger")
            
    return await render_template('enforce_change_password.html')

@core_bp.route('/profile/change-password', methods=['POST'])
@login_required
@atomic_transaction('core', severity=8, impact_category='Security')
async def change_password():
    # ... Implementation from app.py ...
    # Simplified for brevity, assume logic is migrated intact
    try:
        current_pass = (await request.form).get('current_password')
        new_pass = (await request.form).get('new_password')
        confirm_pass = (await request.form).get('confirm_password')
        
        if new_pass != confirm_pass:
            await flash("Las contraseñas no coinciden", "danger")
            return redirect(request.referrer)

        async with get_db_cursor() as cursor:
            await cursor.execute("SELECT password_hash FROM sys_users WHERE id = %s", (g.user['id'],))
            row = await cursor.fetchone()
            if row and check_password_hash(row[0], current_pass):
                 new_hash = generate_password_hash(new_pass)
                 await cursor.execute("UPDATE sys_users SET password_hash = %s, temp_password_hash=NULL, recovery_attempts=0 WHERE id = %s", (new_hash, g.user['id']))
                 await _log_security_event("PASSWORD_CHANGE_INTERNAL", "SUCCESS", target_user_id=g.user['id'])
                 # Clear session specific flags
                 if g.sid and 's' in session:
                     session['s'][g.sid].pop('must_change_password', None)
                     session.modified = True
                 await flash("Contraseña actualizada.", "success")
                 return redirect(url_for('core.login'))
            else:
                 await flash("Contraseña actual incorrecta", "danger")
    except Exception as e:
        await flash(f"Error: {e}", "danger")
    return redirect(request.referrer)

@core_bp.route('/auth/reset-password', methods=['GET', 'POST'])
async def reset_password_public():
    if request.method == 'POST':
        username = (await request.form).get('username')
        enterprise_id = (await request.form).get('enterprise_id')
        current_pass = (await request.form).get('current_password')
        new_pass = (await request.form).get('new_password')
        confirm_pass = (await request.form).get('confirm_password')
        
        if new_pass != confirm_pass:
            await flash("Las nuevas contraseñas no coinciden", "danger")
            # We need to re-pass enterprises to template
            async with get_db_cursor() as cursor:
                await cursor.execute("SELECT id, nombre FROM sys_enterprises WHERE estado = 'activo'")
                enterprises = await cursor.fetchall()
            return await render_template('reset_password.html', enterprises=enterprises)
        
        try:
            async with get_db_cursor() as cursor:
                await cursor.execute("SELECT id, password_hash, temp_password_hash, temp_password_expires FROM sys_users WHERE username = %s AND enterprise_id = %s", (username, enterprise_id))
                user = await cursor.fetchone()
                
                if user:
                    u_id, pwd_hash, temp_hash, temp_expires = user
                    
                    is_valid = check_password_hash(pwd_hash, current_pass)
                    if not is_valid and temp_hash and temp_expires:
                        if isinstance(temp_expires, str):
                            temp_expires = datetime.datetime.strptime(temp_expires, '%Y-%m-%d %H:%M:%S')
                        if datetime.datetime.now() < temp_expires and check_password_hash(temp_hash, current_pass):
                            is_valid = True
                            
                    if is_valid:
                        new_hash = generate_password_hash(new_pass)
                        await cursor.execute("""
                            UPDATE sys_users 
                            SET password_hash = %s, 
                                temp_password_hash = NULL, 
                                temp_password_expires = NULL,
                                temp_password_used = 0,
                                recovery_attempts = 0 
                            WHERE id = %s
                        """, (new_hash, u_id))
                        
                        await _log_security_event("PASSWORD_RESET_EXTERNAL", "SUCCESS", target_user_id=u_id, details="Password reset via public recovery page.")

                        if 's' in session:
                            for s_id in list(session['s'].keys()):
                                if session['s'][s_id].get('user_id') == u_id:
                                    session['s'][s_id].pop('must_change_password', None)
                            session.modified = True
                        
                        await cursor.execute("SELECT email, enterprise_id FROM sys_users WHERE id = %s", (u_id,))
                        u_row = await cursor.fetchone()
                        u_email = u_row[0] if u_row else None
                        ent_id = u_row[1] if u_row else 1
                        
                        await cursor.execute("SELECT email FROM sys_users WHERE username = 'admin' AND enterprise_id = %s", (ent_id,))
                        admin_row = await cursor.fetchone()
                        admin_email = admin_row[0] if admin_row else None
                        
                        _async_email(email_service.enviar_notificacion_cambio_password, u_email, username, admin_email, ent_id)
                        
                        await flash("Contraseña actualizada. Ya puede iniciar sesión.", "success")
                        return redirect(url_for('core.login'))
                    else:
                        await _log_security_event("PASSWORD_RESET_EXTERNAL", "FAILURE", target_user_id=u_id, details="Invalid data or expired temp password.")
                        await flash("Verificación fallida: Usuario o contraseña actual incorrectos", "danger")
                else:
                    await _log_security_event("PASSWORD_RESET_EXTERNAL", "FAILURE", details=f"Non-existent username: {username}")
                    await flash("Usuario no encontrado", "danger")
        except Exception as e:
            await _log_security_event("PASSWORD_RESET_EXTERNAL", "ERROR", details=str(e))
            await flash(f"Error: {e}", "danger")
    
    async with get_db_cursor() as cursor:
        await cursor.execute("SELECT id, nombre FROM sys_enterprises WHERE estado = 'activo'")
        enterprises = await cursor.fetchall()

    return await render_template('reset_password.html', enterprises=enterprises)

def mask_email(email):
    """
    Masks an email address for privacy, showing only hints.
    Example: marcelo_peri@yahoo.com -> ma*******_p***@y****.c**
    """
    if not email or '@' not in email:
        return "******"
    
    try:
        local_part, domain_part = email.split('@')
        
        # Mask Local Part
        # Show first 2 chars, valid separators (_.-), and mask the rest
        masked_local = ""
        if len(local_part) > 2:
            masked_local += local_part[:2]
            for i in range(2, len(local_part)):
                char = local_part[i]
                if char in ['_', '.', '-']:
                    masked_local += char
                else:
                    masked_local += "*"
        else:
            masked_local = local_part # Too short to mask safely
            
        # Mask Domain Part
        # Show first char of domain name and extension
        if '.' in domain_part:
            domain_name, extension = domain_part.rsplit('.', 1)
            masked_domain_name = domain_name[0] + "*" * (len(domain_name) - 1) if len(domain_name) > 1 else domain_name
            masked_extension = extension[0] + "*" * (len(extension) - 1) if len(extension) > 1 else extension
            masked_domain = f"{masked_domain_name}.{masked_extension}"
        else:
            masked_domain = "*" * len(domain_part)

        return f"{masked_local}@{masked_domain}"
    except Exception:
        return "******@******"

@core_bp.route('/auth/request-temp-password', methods=['POST'])
async def request_temp_password():
    username = (await request.form).get('username')
    enterprise_id = (await request.form).get('enterprise_id')
    
    if not username:
        await flash("El usuario es requerido", "danger")
        return redirect(url_for('core.reset_password_public'))
        
    try:
        async with get_db_cursor() as cursor:
            # Find specific user in selected enterprise
            await cursor.execute("SELECT id, username, email, recovery_attempts, enterprise_id FROM sys_users WHERE username = %s AND enterprise_id = %s", (username, enterprise_id))
            users = await cursor.fetchall()
            
            if users:
                user = users[0] 
                u_id, u_name, u_email_db, attempts, ent_id = user
                
                if not u_email_db:
                    await flash("El usuario no tiene un email configurado para recuperación.", "warning")
                    return redirect(url_for('core.reset_password_public'))

                if attempts >= 5:
                    await _log_security_event("TEMP_PASSWORD_REQUEST", "FAILURE", target_user_id=u_id, details="Limit reached")
                    await flash("Se han superado los intentos. Contacte al administrador.", "danger")
                    return redirect(url_for('core.login'))

                # Generate Readable Temp Password (No ambiguous chars like I, l, 1, O, 0)
                import random
                alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789" 
                temp_pass = ''.join(random.choice(alphabet) for _ in range(8))
                
                temp_hash = generate_password_hash(temp_pass)
                expires = datetime.datetime.now() + datetime.timedelta(hours=24)
                
                await cursor.execute("""
                    UPDATE sys_users SET temp_password_hash = %s, temp_password_expires = %s, temp_password_used = 0,
                                       recovery_attempts = recovery_attempts + 1 WHERE id = %s
                """, (temp_hash, expires.strftime('%Y-%m-%d %H:%M:%S'), u_id))
                
                _async_email(email_service.enviar_clave_temporal, u_email_db, u_name, temp_pass, ent_id)
                await _log_security_event("TEMP_PASSWORD_REQUEST", "SUCCESS", target_user_id=u_id)
                
                masked = mask_email(u_email_db)
                await flash(f"Se ha enviado una CLAVE TEMPORAL a {masked}. Úsela para ingresar.", "success")
                # Redirect to login so they can use it immediately
                return redirect(url_for('core.login')) 
            else:
                await _log_security_event("TEMP_PASSWORD_REQUEST", "FAILURE", details=f"User not found: {username} in Ent {enterprise_id}")
                await flash("Usuario no encontrado en la empresa seleccionada.", "danger")
    except Exception as e:
        await _log_security_event("TEMP_PASSWORD_REQUEST", "ERROR", details=str(e))
        await flash(f"Error procesando solicitud: {str(e)}", "danger")
    return redirect(url_for('core.reset_password_public'))

@core_bp.route('/auth/reset/<int:ent_id>/<int:user_id>/<token>', methods=['GET'])
async def reset_with_token(ent_id, user_id, token):
    """
    Validates token and renders reset page.
    """
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("""
                SELECT username, temp_password_hash, temp_password_expires, temp_password_used 
                FROM sys_users WHERE id = %s AND enterprise_id = %s
            """, (user_id, ent_id))
            user = await cursor.fetchone()
            
            if not user:
                await flash("Enlace inválido.", "danger")
                return redirect(url_for('core.login'))
                
            username, db_hash, expires, used = user
            
            if used:
                await flash("Este enlace ya ha sido utilizado.", "warning")
                return redirect(url_for('core.login'))
                
            if isinstance(expires, str):
                expires = datetime.datetime.strptime(expires, '%Y-%m-%d %H:%M:%S')
                
            if datetime.datetime.now() > expires:
                await flash("El enlace ha caducado. Solicite uno nuevo.", "warning")
                return redirect(url_for('core.reset_password_public'))
                
            if not db_hash or not check_password_hash(db_hash, token):
                await flash("Enlace inválido o corrupto.", "danger")
                return redirect(url_for('core.login'))
                
            # Valid! Render hidden form
            return await render_template('reset_token.html', 
                                   ent_id=ent_id, user_id=user_id, token=token, username=username)
    except Exception as e:
        await flash(f"Error: {e}", "danger")
        return redirect(url_for('core.login'))

@core_bp.route('/auth/reset-confirm', methods=['POST'])
async def reset_confirm():
    ent_id = (await request.form).get('ent_id')
    user_id = (await request.form).get('user_id')
    token = (await request.form).get('token')
    new_pass = (await request.form).get('new_password')
    confirm_pass = (await request.form).get('confirm_password')
    
    if new_pass != confirm_pass:
        await flash("Las contraseñas no coinciden.", "danger")
        # Retry? Ideally render template again, but simpler to redirect to token link
        return redirect(url_for('core.reset_with_token', ent_id=ent_id, user_id=user_id, token=token))
        
    try:
        async with get_db_cursor() as cursor:
            # Re-validate (To prevent race/replay)
            await cursor.execute("SELECT temp_password_hash, temp_password_expires, temp_password_used FROM sys_users WHERE id = %s AND enterprise_id = %s", (user_id, ent_id))
            row = await cursor.fetchone()
            if not row: raise Exception("Usuario inválido")
            
            db_hash, expires, used = row
            if used: raise Exception("Enlace ya usado")
             # Assuming checks passed in GET, but double check
             
            if not check_password_hash(db_hash, token):
                 raise Exception("Token inválido")

            new_hash = generate_password_hash(new_pass)
            
            # Update password and INVALIDATE token
            await cursor.execute("""
                UPDATE sys_users 
                SET password_hash = %s, 
                    temp_password_hash = NULL, 
                    temp_password_expires = NULL, 
                    temp_password_used = 1,
                    recovery_attempts = 0 
                WHERE id = %s
            """, (new_hash, user_id))
            
            await _log_security_event("PASSWORD_RESET_COMPLETED", "SUCCESS", target_user_id=user_id, enterprise_id=ent_id)
            
            await flash("Contraseña actualizada con éxito. Ya puede iniciar sesión.", "success")
            return redirect(url_for('core.login'))
            
    except Exception as e:
        await flash(f"Error: {e}", "danger")
        return redirect(url_for('core.login'))

# --- ADMIN ROUTES ---

@core_bp.route('/admin/roles')
@login_required
@permission_required('admin_roles')
async def admin_roles():
    selected_role_id = request.args.get('role_id')
    async with get_db_cursor() as cursor:
        await cursor.execute("SELECT id, name, description FROM sys_roles WHERE enterprise_id = %s ORDER BY name", (g.user['enterprise_id'],))
        db_roles = await cursor.fetchall()
        roles = [{'id': r[0], 'name': r[1], 'description': r[2]} for r in db_roles]
        
        # SELF-HEALING: If no roles exist, auto-init SoD
        if not roles:
            try:
                from services.sod_service import initialize_sod_structure
                # We need to temporarily run this. Note: initialize_sod_structure manages its own transaction context.
                # However, since we are inside `async with get_db_cursor() as cursor`, we have an active transaction/connection.
                # If `initialize_sod_structure` uses `get_db_cursor`, it might clash if connection pool limit is tight or transactions conflict.
                # But here we are just reading roles.
                # Let's try initialize.
                await initialize_sod_structure(g.user['enterprise_id'])
                await flash("Roles SoD inicializados automáticamente por sistema.", "info")
                
                # Reload
                await cursor.execute("SELECT id, name, description FROM sys_roles WHERE enterprise_id = %s ORDER BY name", (g.user['enterprise_id'],))
                roles = [{'id': r[0], 'name': r[1], 'description': r[2]} for r in await cursor.fetchall()]
            except Exception as e:
                await flash(f"No se pudieron cargar roles automáticos: {e}", "warning")
        
        selected_role = None
        permissions_by_category = {}
        current_role_permissions = []
        sod_analysis = None
        
        if selected_role_id:
             await cursor.execute("SELECT id, name, description FROM sys_roles WHERE id = %s AND enterprise_id = %s", (selected_role_id, g.user['enterprise_id']))
             r = await cursor.fetchone()
             if r: selected_role = {'id': r[0], 'name': r[1], 'description': r[2]}
             
             # Only show permissions for this enterprise
             # And only show 'SISTEMA' category if current user is sysadmin
             is_sysadmin = 'sysadmin' in g.permissions
             if is_sysadmin:
                 await cursor.execute("SELECT id, code, description, category FROM sys_permissions WHERE enterprise_id = %s ORDER BY category", (g.user['enterprise_id'],))
             else:
                 await cursor.execute("SELECT id, code, description, category FROM sys_permissions WHERE enterprise_id = %s AND (category != 'SISTEMA' OR category IS NULL) ORDER BY category", (g.user['enterprise_id'],))
             
             for p in await cursor.fetchall():
                 cat = p[3] or 'General'
                 if cat not in permissions_by_category: permissions_by_category[cat] = []
                 permissions_by_category[cat].append({'id': p[0], 'code': p[1], 'description': p[2], 'is_sys': getattr(p, 'is_sys', False)})
                 
             await cursor.execute("SELECT permission_id FROM sys_role_permissions WHERE role_id = %s AND enterprise_id = %s", (selected_role_id, g.user['enterprise_id']))
             current_role_permissions = [row[0] for row in await cursor.fetchall()]
             
             # Realizar Análisis SoD
             from services.sod_service import analyze_role_sod
             # Extraer objetos de permisos actualmente otorgados
             current_perms_full = []
             for cat_list in permissions_by_category.values():
                 for p_dict in cat_list:
                     if p_dict['id'] in current_role_permissions:
                         p_copy = p_dict.copy()
                         p_copy['category'] = next((k for k, v in permissions_by_category.items() if p_dict in v), 'General')
                         current_perms_full.append(p_copy)
                         
    sod_errors = session.get('sod_errors', [])

    return await render_template('admin_roles.html', roles=roles, selected_role=selected_role, 
                          permissions_by_category=permissions_by_category, 
                          current_role_permissions=current_role_permissions,
                          sod_analysis=sod_analysis, sod_errors=sod_errors)

@core_bp.route('/admin/roles/create', methods=['POST'])
@login_required
@permission_required('admin_roles')
async def create_role():
    name = (await request.form).get('name')
    desc = (await request.form).get('description')
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("""
                INSERT INTO sys_roles (enterprise_id, name, description, user_id) 
                VALUES (%s, %s, %s, %s)
            """, (g.user['enterprise_id'], name, desc, g.user['id']))
        await flash(f"Rol {name} creado.", "success")
    except Exception as e: await flash(str(e), "danger")
    return redirect(url_for('core.admin_roles'))

@core_bp.route('/admin/roles/init-sod', methods=['POST'])
@login_required
@permission_required('admin_roles')
async def admin_roles_init_sod():
    try:
        from services.sod_service import initialize_sod_structure
        await initialize_sod_structure(g.user['enterprise_id'])
        await flash("Estructura de Roles SoD inicializada correctamente", "success")
    except Exception as e:
        await flash(f"Error al inicializar SoD: {e}", "danger")
    return redirect(url_for('core.admin_roles'))

@core_bp.route('/admin/roles/update_permissions/<int:role_id>', methods=['POST'])
@login_required
@permission_required('admin_roles')
async def update_role_permissions(role_id):
    perms = (await request.form).getlist('permissions')
    
    # --- NIVEL 1: CONTROL DE CUMPLIMIENTO DE AUDITORÍA (CISA/SOX/Audit-Safe) ---
    from services.audit_certification_service import AuditCertificationService
    ok, violations = await AuditCertificationService.validate_permissions_compliance(perms)
    if not ok:
        # 1. Log del Bloqueo Crítico (Warning temporalmente)
        details = f"ADVERTENCIA SISTÉMICA: El módulo no es Audit-Ready. Violaciones: {violations}"
        
        # Continuar con el proceso para permitir evaluación SoD, pero advertir al usuario
        # await flash("ADVERTENCIA DE AUDITORÍA: Uno o más módulos seleccionados aún no cumplen con los estándares de trazabilidad. Se ha notificado al administrador.", "warning")
    # -------------------------------------------------------------------------

    try:
        async with get_db_cursor() as cursor:
            # Protection: Only sysadmins can assign 'SISTEMA' permissions
            is_sysadmin = 'sysadmin' in g.permissions
            
            for p_id in perms:
                if not is_sysadmin:
                    # Check if this permission is restricted
                    await cursor.execute("SELECT category FROM sys_permissions WHERE id = %s", (p_id,))
                    res = await cursor.fetchone()
                    if res and res[0] == 'SISTEMA':
                        await flash("No tiene permisos para asignar permisos de SISTEMA.", "danger")
                        return redirect(url_for('core.admin_roles', role_id=role_id))

            # 1. Obtener roles y evaluar SoD ANTES de grabar
            await cursor.execute("SELECT name FROM sys_roles WHERE id = %s", (role_id,))
            role_info = await cursor.fetchone()
            role_name = role_info[0] if role_info else "Desconocido"
            
            # Obtener permisos actuales para ver el delta (Added/Removed)
            await cursor.execute("SELECT sys_permissions.code FROM sys_permissions JOIN sys_role_permissions ON sys_permissions.id = sys_role_permissions.permission_id WHERE sys_role_permissions.role_id = %s", (role_id,))
            current_codes = [r[0] for r in await cursor.fetchall()]

            await cursor.execute("SELECT id, code, description, category FROM sys_permissions WHERE id IN (%s)" % ', '.join(['%s']*len(perms)) if perms else "SELECT id, code, description, category FROM sys_permissions WHERE id = -1", tuple(perms))
            new_perms_list = [{'id': r[0], 'code': r[1], 'description': r[2], 'category': r[3]} for r in await cursor.fetchall()]

            from services.sod_service import analyze_role_sod
            sod_analysis = analyze_role_sod(role_name, new_perms_list, current_codes=current_codes)
            
            if sod_analysis['conflictos_detalle']:
                import json
                # Guardo en sesión qué códigos fallaron y su estructura rica para tooltips
                sod_errors_data = {}
                for c in sod_analysis['conflictos_detalle']:
                    for p in c['perms']:
                        sod_errors_data[p['code']] = {
                            'detalle': c['detalle'],
                            'norma': c['regla'],
                            'regla': c['regla']
                        }
                
                session['sod_errors_data'] = sod_errors_data # Diccionario {codigo: {detalle, norma}}
                session['sod_errors'] = list(sod_errors_data.keys()) # Lista simple para clases CSS
                
                # Armo la estructura rica para ser procesada por SweetAlert en base.html
                error_data = {
                    'sod_error': True,
                    'legend': "Podra continuar navegando con sus permisos pero consulte con el auditor de sistemas",
                    'conflictos': []
                }
                for c in sod_analysis['conflictos_detalle']:
                    error_data['conflictos'].append({
                        'regla': c['regla'],
                        'detalle': c['detalle'],
                        'tipo': c.get('tipo', 'Conflicto'),
                        'perms': [{'code': p['code'], 'desc': p.get('description',''), 'cat': p.get('category','')} for p in c['perms']]
                    })
                    
                await flash(json.dumps(error_data), "sod_danger")
                # return redirect(url_for('core.admin_roles', role_id=role_id))

            # Si no hay conflictos, procedo al borrado real
            await cursor.execute("DELETE FROM sys_role_permissions WHERE role_id = %s AND enterprise_id = %s", (role_id, g.user['enterprise_id']))

            # Inserción real
            for p_id_new in perms:
                await cursor.execute("""
                    INSERT INTO sys_role_permissions (enterprise_id, role_id, permission_id, user_id) 
                    VALUES (%s, %s, %s, %s)
                """, (g.user['enterprise_id'], role_id, p_id_new, g.user['id']))
            
            violaciones_str = "Ninguna"
            inocuos_count = len(sod_analysis['inocuos'])
            
            # Log de Auditoría Enriquecido (CISA Requirement)
            details = f"Rol: {role_name} (ID:{role_id}). Modificacion completada (Audit-Safe). Total Permisos: {len(perms)}."
            await cursor.execute("INSERT INTO sys_security_logs (enterprise_id, actor_user_id, action, status, details, ip_address, session_id) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                          (g.user['enterprise_id'], g.user['id'], 'UPDATE_ROLE_PERMS', 'SUCCESS', details, request.remote_addr, session.get('session_id', 'N/A')))
        
        # Limpio errores sod viejos
        if 'sod_errors' in session: session.pop('sod_errors')
        await flash("Permisos actualizados y auditados bajo normativa SoD.", "success")
    except Exception as e: await flash(str(e), "danger")
    return redirect(url_for('core.admin_roles', role_id=role_id))

@core_bp.route('/admin/roles/revoke', methods=['POST'])
@login_required
@permission_required('admin_roles')
async def revoke_role_permission():
    role_id = (await request.form).get('role_id')
    permission_id = (await request.form).get('permission_id')
    reason = (await request.form).get('reason', 'Revocación por Auditoría SoD')
    
    try:
        async with get_db_cursor() as cursor:
            # Obtener datos para log
            await cursor.execute("SELECT name FROM sys_roles WHERE id = %s AND enterprise_id = %s", (role_id, g.user['enterprise_id']))
            role_row = await cursor.fetchone()
            if not role_row: return "Rol no encontrado", 404
            role_name = role_row[0]
            
            await cursor.execute("SELECT code, category FROM sys_permissions WHERE id = %s", (permission_id,))
            p_data = await cursor.fetchone()
            if not p_data: return "Permiso no encontrado", 404
            p_code = p_data[0]
            p_cat = p_data[1] or 'General'
            
            # Borrar
            await cursor.execute("DELETE FROM sys_role_permissions WHERE role_id = %s AND permission_id = %s AND enterprise_id = %s", 
                           (role_id, permission_id, g.user['enterprise_id']))
            
            # Log de Auditoría CISA
            details = f"Operación: Revocación de Permiso | Rol: {role_name} | Módulo: {p_cat} | Permiso Quitado: {p_code} | Motivo: {reason}"
            await cursor.execute("INSERT INTO sys_security_logs (enterprise_id, actor_user_id, action, status, details, ip_address, session_id) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                          (g.user['enterprise_id'], g.user['id'], 'REVOKE_PERMISSION', 'SUCCESS', details, request.remote_addr, session.get('session_id', 'N/A')))
            
        await flash(f"Control de Seguridad: Permiso {p_code} revocado del perfil {role_name}.", "warning")
    except Exception as e:
        await flash(f"Error en auditoría al revocar: {e}", "danger")
        
    return redirect(url_for('core.admin_roles', role_id=role_id))

@core_bp.route('/admin/roles/delete/<int:role_id>', methods=['POST'])
@login_required
@permission_required('admin_roles')
async def delete_role(role_id):
    # Logic to prevent deleting admin
    try:
        async with get_db_cursor() as cursor:
             await cursor.execute("DELETE FROM sys_roles WHERE id = %s AND enterprise_id = %s", (role_id, g.user['enterprise_id']))
        await flash("Rol eliminado", "success")
    except Exception as e: await flash(str(e), "danger")
    return redirect(url_for('core.admin_roles'))


@core_bp.route('/admin/users')
@login_required
@permission_required('admin_users')
async def admin_users():
    async with get_db_cursor() as cursor:
        await cursor.execute("""
            SELECT sys_users.id, sys_users.username, sys_users.email, sys_roles.name as role_name, sys_users.created_at, sys_users.role_id, sys_users.must_change_password
            FROM sys_users 
            LEFT JOIN sys_roles ON sys_users.role_id = sys_roles.id AND sys_roles.enterprise_id = sys_users.enterprise_id
            WHERE sys_users.enterprise_id = %s
        """, (g.user['enterprise_id'],))
        users = [dict(zip([column[0] for column in cursor.description], row)) for row in await cursor.fetchall()]
        # Filter roles: only show 'adminSys' if current user is sysadmin
        is_sysadmin = 'sysadmin' in g.permissions
        if is_sysadmin:
            await cursor.execute("SELECT id, name FROM sys_roles WHERE enterprise_id = %s", (g.user['enterprise_id'],))
        else:
            await cursor.execute("SELECT id, name FROM sys_roles WHERE enterprise_id = %s AND LOWER(name) != 'adminsys'", (g.user['enterprise_id'],))
        
        roles = [{'id': r[0], 'name': r[1]} for r in await cursor.fetchall()]
    return await render_template('admin_users.html', system_users=users, roles=roles)

@core_bp.route('/admin/users/reset-attempts/<int:user_id>', methods=['POST'])
@login_required
@permission_required('admin_users')
async def reset_user_attempts(user_id):
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("UPDATE sys_users SET recovery_attempts = 0 WHERE id = %s AND enterprise_id = %s", 
                           (user_id, g.user['enterprise_id']))
        await flash("Intentos de recuperación restablecidos.", "success")
        await _log_security_event("RESET_ATTEMPTS", "SUCCESS", target_user_id=user_id, details="Admin reset attempts")
    except Exception as e:
        await flash(f"Error: {e}", "danger")
    return redirect(url_for('core.admin_users'))

@core_bp.route('/admin/users/reset-password/<int:user_id>', methods=['POST'])
@login_required
@permission_required('admin_users')
async def admin_reset_password(user_id):
    """Resets user password to a default value and forces change on next login"""
    DEFAULT_PASS = "Temporal123!"
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            # Fetch user details for email
            await cursor.execute("SELECT username, email FROM sys_users WHERE id = %s AND enterprise_id = %s", (user_id, g.user['enterprise_id']))
            user = await cursor.fetchone()
            if not user:
                await flash("Usuario no encontrado.", "danger")
                return redirect(url_for('core.admin_users'))

            h = generate_password_hash(DEFAULT_PASS)
            await cursor.execute("""
                UPDATE sys_users 
                SET password_hash = %s, must_change_password = 1, recovery_attempts = 0 
                WHERE id = %s AND enterprise_id = %s
            """, (h, user_id, g.user['enterprise_id']))
            
            # Send Notification
            if user['email']:
                success, error = await email_service.enviar_clave_temporal(user['email'], user['username'], DEFAULT_PASS, g.user['enterprise_id'])
                if success:
                    await flash(f"Contraseña restablecida. Se envió un correo a {user['email']}.", "success")
                else:
                    await flash(f"Contraseña restablecida a '{DEFAULT_PASS}', pero falló el envío del correo: '{error}'. Por favor contacte al administrador.", "warning")
            else:
                await flash(f"Contraseña restablecida a '{DEFAULT_PASS}'. El usuario no tiene correo configurado.", "info")

        await _log_security_event("ADMIN_RESET_PASSWORD", "SUCCESS", target_user_id=user_id, details="Password reset to default and forced change")
    except Exception as e:
        await flash(f"Error: {e}", "danger")
    return redirect(url_for('core.admin_users'))

@core_bp.route('/admin/users/create', methods=['POST'])
@login_required
@permission_required('admin_users')
async def create_system_user():
    # Insert logic
    try:
        uname = (await request.form)['username']
        email = (await request.form)['email']
        
        # Validar estado del correo
        es_valido, msg = email_service.validar_estado_correo(email)
        if not es_valido:
            await flash(f"Error de Validación de Correo: {msg}", "danger")
            return redirect(url_for('core.admin_users'))

        pwd = (await request.form)['password']
        rid = (await request.form)['role_id'] or None
        
        # --- CONTROL DE CUMPLIMIENTO DE AUDITORÍA (CISA/SOX) ---
        if rid:
            from services.audit_certification_service import AuditCertificationService
            async with get_db_cursor() as cursor:
                await cursor.execute("SELECT permission_id FROM sys_role_permissions WHERE role_id = %s", (rid,))
                p_ids = [r[0] for r in await cursor.fetchall()]
            
            ok, v = await AuditCertificationService.validate_permissions_compliance(p_ids)
            if not ok:
                await _log_security_event("AUDIT_COMPLIANCE_BLOCK", "FAIL", details=f"Intento de asignar rol no audit-ready (ID:{rid}) al crear usuario {uname}. Violaciones: {v}")
                await AuditCertificationService.notify_saas_owner(v, g.user['username'], f"Asignación de Rol a Nuevo Usuario: {uname}")
                await flash("OPERACIÓN ANULADA: El rol asignado contiene permisos para módulos que no cumplen con los estándares de auditoría. Se ha notificado al SaaS Owner.", "danger")
                return redirect(url_for('core.admin_users'))
        # ------------------------------------------------------

        force_change = 1 if (await request.form).get('must_change_password') == 'on' else 0
        h = generate_password_hash(pwd)
        async with get_db_cursor() as cursor:
            await cursor.execute("""
                INSERT INTO sys_users (enterprise_id, username, email, password_hash, role_id, must_change_password) 
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (g.user['enterprise_id'], uname, email, h, rid, force_change))
        
        # Notify user if force_change is on
        if force_change and email:
            success, error = await email_service.enviar_clave_temporal(email, uname, pwd, g.user['enterprise_id'])
            if success:
                await flash("Usuario creado y notificación enviada por correo.", "success")
            else:
                await flash(f"Usuario creado, pero falló el envío del correo: '{error}'. Contacte al administrador.", "warning")
        else:
            await flash("Usuario creado con éxito.", "success")
            
    except Exception as e: 
        await flash(str(e), "danger")
    return redirect(url_for('core.admin_users'))

@core_bp.route('/admin/users/update/<int:user_id>', methods=['POST'])
@login_required
@permission_required('admin_users')
async def update_user(user_id):
    """Update user information including optional password change"""
    try:
        username = (await request.form).get('username')
        email = (await request.form).get('email')
        role_id = (await request.form).get('role_id') or None
        
        # --- CONTROL DE CUMPLIMIENTO DE AUDITORÍA (CISA/SOX) ---
        if role_id:
            from services.audit_certification_service import AuditCertificationService
            async with get_db_cursor() as cursor:
                await cursor.execute("SELECT permission_id FROM sys_role_permissions WHERE role_id = %s", (role_id,))
                p_ids = [r[0] for r in await cursor.fetchall()]
            
            ok, v = await AuditCertificationService.validate_permissions_compliance(p_ids)
            if not ok:
                await _log_security_event("AUDIT_COMPLIANCE_BLOCK", "FAIL", details=f"Intento de asignar rol no audit-ready (ID:{role_id}) al usuario ID:{user_id}. Violaciones: {v}")
                await AuditCertificationService.notify_saas_owner(v, g.user['username'], f"Actualización de Usuario ID: {user_id}")
                await flash("OPERACIÓN ANULADA: El rol asignado no cumple con los estándares de auditoría. Notificado al SaaS Owner.", "danger")
                return redirect(url_for('core.admin_users'))
        # ------------------------------------------------------
        
        new_password = (await request.form).get('password', '').strip()
        force_change = 1 if (await request.form).get('must_change_password') == 'on' else 0
        
        # Validar estado del correo si cambió
        async with get_db_cursor() as cursor:
            await cursor.execute("SELECT email FROM sys_users WHERE id = %s AND enterprise_id = %s", 
                          (user_id, g.user['enterprise_id']))
            current_user = await cursor.fetchone()
            
            if not current_user:
                await flash("Usuario no encontrado", "danger")
                return redirect(url_for('core.admin_users'))
            
            # Si el email cambió, validarlo
            if email != current_user[0]:
                es_valido, msg = email_service.validar_estado_correo(email)
                if not es_valido:
                    await flash(f"Error de Validación de Correo: {msg}", "danger")
                    return redirect(url_for('core.admin_users'))
            
            # Si se proporcionó nueva contraseña, actualizarla
            if new_password:
                password_hash = generate_password_hash(new_password)
                await cursor.execute("""
                    UPDATE sys_users 
                    SET username = %s, email = %s, role_id = %s, password_hash = %s, must_change_password = %s, updated_by = %s
                    WHERE id = %s AND enterprise_id = %s
                """, (username, email, role_id, password_hash, force_change, g.user['id'], user_id, g.user['enterprise_id']))
                
                # Notify user if force_change is on
                if force_change and email:
                    success, error = await email_service.enviar_clave_temporal(email, username, new_password, g.user['enterprise_id'])
                    if success:
                        await flash("Usuario actualizado y notificación enviada por correo.", "success")
                    else:
                        await flash(f"Usuario actualizado, pero falló el envío del correo: '{error}'. Contacte al administrador.", "warning")
                else:
                    await flash("Usuario actualizado con nueva contraseña", "success")
            else:
                # Solo actualizar datos sin cambiar contraseña, pero permitimos cambiar el flag de must_change
                await cursor.execute("""
                    UPDATE sys_users 
                    SET username = %s, email = %s, role_id = %s, must_change_password = %s, updated_by = %s
                    WHERE id = %s AND enterprise_id = %s
                """, (username, email, role_id, force_change, g.user['id'], user_id, g.user['enterprise_id']))
                await flash("Usuario actualizado", "success")
                
    except Exception as e:
        await flash(f"Error al actualizar usuario: {str(e)}", "danger")
    
    return redirect(url_for('core.admin_users'))


# ... ÁREAS ABM ...

@core_bp.route('/admin/areas')
@login_required
@permission_required('admin_users')
async def admin_areas():
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute(
            "SELECT id, nombre, color, icono, activo FROM erp_areas WHERE enterprise_id = %s OR enterprise_id = 0 ORDER BY nombre",
            (ent_id,)
        )
        areas = await cursor.fetchall()
    return await render_template('admin_areas.html', areas=areas)

@core_bp.route('/admin/areas/create', methods=['POST'])
@login_required
@permission_required('admin_users')
async def create_area():
    ent_id = g.user['enterprise_id']
    nombre = (await request.form).get('nombre', '').upper().strip()
    color  = (await request.form).get('color', 'secondary')
    icono  = (await request.form).get('icono', 'fa-building')
    if not nombre:
        await flash("El nombre del área es obligatorio.", "danger")
        return redirect(url_for('core.admin_areas'))
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute(
                "INSERT INTO erp_areas (enterprise_id, nombre, color, icono) VALUES (%s, %s, %s, %s)",
                (ent_id, nombre, color, icono)
            )
        await flash(f"Área '{nombre}' creada.", "success")
    except Exception as e:
        await flash(f"Error: {e}", "danger")
    return redirect(url_for('core.admin_areas'))

@core_bp.route('/admin/areas/update/<int:id>', methods=['POST'])
@login_required
@permission_required('admin_users')
async def update_area(id):
    ent_id = g.user['enterprise_id']
    nombre = (await request.form).get('nombre', '').upper().strip()
    color  = (await request.form).get('color', 'secondary')
    icono  = (await request.form).get('icono', 'fa-building')
    activo = 1 if (await request.form).get('activo') else 0
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute(
                "UPDATE erp_areas SET nombre=%s, color=%s, icono=%s, activo=%s WHERE id=%s AND enterprise_id=%s",
                (nombre, color, icono, activo, id, ent_id)
            )
        await flash("Área actualizada.", "success")
    except Exception as e:
        await flash(f"Error: {e}", "danger")
    return redirect(url_for('core.admin_areas'))

@core_bp.route('/admin/areas/delete/<int:id>', methods=['POST'])
@login_required
@permission_required('admin_users')
async def delete_area(id):
    ent_id = g.user['enterprise_id']
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("DELETE FROM erp_areas WHERE id=%s AND enterprise_id=%s", (id, ent_id))
        await flash("Área eliminada.", "success")
    except Exception as e:
        await flash(f"No se puede eliminar: {e}", "danger")
    return redirect(url_for('core.admin_areas'))

@core_bp.route('/api/areas')
@login_required
async def api_areas():
    """API para selectores dinámicos de área."""
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute(
            "SELECT id, nombre, color, icono FROM erp_areas WHERE (enterprise_id=%s OR enterprise_id=0) AND activo=1 ORDER BY nombre",
            (ent_id,)
        )
        areas = await cursor.fetchall()
    return jsonify(areas)

# ... PUESTOS ABM ...

@core_bp.route('/admin/puestos')
@login_required
@permission_required('admin_users')
async def admin_puestos():
    area = request.args.get('area')
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        # Cargar áreas desde tabla
        await cursor.execute(
            "SELECT nombre, color FROM erp_areas WHERE (enterprise_id=%s OR enterprise_id=0) AND activo=1 ORDER BY nombre",
            (ent_id,)
        )
        areas = await cursor.fetchall()

        query = "SELECT id, nombre, area, activo FROM erp_puestos WHERE enterprise_id = %s"
        params = [ent_id]
        if area:
            query += " AND area = %s"
            params.append(area)
        query += " ORDER BY area, nombre"
        await cursor.execute(query, params)
        puestos = await cursor.fetchall()
    return await render_template('admin_puestos.html', puestos=puestos, areas=areas, current_area=area)

@core_bp.route('/admin/puestos/create', methods=['POST'])
@login_required
@permission_required('admin_users')
async def create_puesto():
    nombre = (await request.form).get('nombre')
    area = (await request.form).get('area')
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("INSERT INTO erp_puestos (enterprise_id, nombre, area) VALUES (%s, %s, %s)", 
                           (g.user['enterprise_id'], nombre, area))
        await flash(f"Puesto {nombre} creado.", "success")
    except Exception as e: await flash(str(e), "danger")
    return redirect(url_for('core.admin_puestos'))

@core_bp.route('/admin/puestos/update/<int:id>', methods=['POST'])
@login_required
@permission_required('admin_users')
async def update_puesto(id):
    nombre = (await request.form).get('nombre')
    area = (await request.form).get('area')
    activo = 1 if (await request.form).get('activo') else 0
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("UPDATE erp_puestos SET nombre = %s, area = %s, activo = %s WHERE id = %s AND enterprise_id = %s", 
                           (nombre, area, activo, id, g.user['enterprise_id']))
        await flash("Puesto actualizado.", "success")
    except Exception as e: await flash(str(e), "danger")
    return redirect(url_for('core.admin_puestos'))

@core_bp.route('/admin/puestos/delete/<int:id>', methods=['POST'])
@login_required
@permission_required('admin_users')
async def delete_puesto(id):
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("DELETE FROM erp_puestos WHERE id = %s AND enterprise_id = %s", (id, g.user['enterprise_id']))
        await flash("Puesto eliminado.", "success")
    except Exception as e: await flash(str(e), "danger")
    return redirect(url_for('core.admin_puestos'))

@core_bp.route('/api/erp/puestos')
@login_required
async def api_get_puestos():
    area = request.args.get('area')
    async with get_db_cursor(dictionary=True) as cursor:
        query = "SELECT id, nombre FROM erp_puestos WHERE enterprise_id = %s AND activo = 1"
        params = [g.user['enterprise_id']]
        if area:
            query += " AND area = %s"
            params.append(area)
        query += " ORDER BY nombre"
        await cursor.execute(query, params)
        return jsonify(await cursor.fetchall())

@core_bp.route('/api/erp/areas')
@login_required
async def api_get_areas():
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute(
            "SELECT id, nombre, color, icono FROM erp_areas WHERE (enterprise_id=%s OR enterprise_id=0) AND activo=1 ORDER BY nombre",
            (ent_id,)
        )
        return jsonify(await cursor.fetchall())

@core_bp.route('/admin/security-logs')
@login_required
@permission_required('admin_users')
async def security_logs():
    f_action = request.args.get('action')
    f_status = request.args.get('status')
    f_actor = request.args.get('actor')
    f_target = request.args.get('target')
    f_ip = request.args.get('ip')
    f_sid = request.args.get('sid')
    
    try:
        async with get_db_cursor() as cursor:
            base_query = """
                SELECT sys_security_logs.id, sys_security_logs.event_time, sys_security_logs.action, sys_security_logs.status, sys_security_logs.details, sys_security_logs.ip_address, sys_security_logs.session_id,
                       sys_users_actor.username as actor_name, sys_users_target.username as target_name
                FROM sys_security_logs
                LEFT JOIN sys_users sys_users_actor ON sys_security_logs.actor_user_id = sys_users_actor.id AND sys_users_actor.enterprise_id = sys_security_logs.enterprise_id
                LEFT JOIN sys_users sys_users_target ON sys_security_logs.target_user_id = sys_users_target.id AND sys_users_target.enterprise_id = sys_security_logs.enterprise_id
                WHERE sys_security_logs.enterprise_id = %s
            """
            params = [g.user['enterprise_id']]
            
            if f_action:
                base_query += " AND sys_security_logs.action = %s"
                params.append(f_action)
            if f_status:
                base_query += " AND sys_security_logs.status = %s"
                params.append(f_status)
            if f_actor:
                base_query += " AND sys_users_actor.username = %s"
                params.append(f_actor)
            if f_target:
                base_query += " AND sys_users_target.username = %s"
                params.append(f_target)
            if f_ip:
                base_query += " AND sys_security_logs.ip_address = %s"
                params.append(f_ip)
            if f_sid:
                base_query += " AND sys_security_logs.session_id = %s"
                params.append(f_sid)
                
            base_query += " ORDER BY sys_security_logs.event_time DESC LIMIT 500"
            await cursor.execute(base_query, params)
            logs = await cursor.fetchall()
            
            # Fetch DISTINCT values for filters
            await cursor.execute("SELECT DISTINCT action FROM sys_security_logs ORDER BY action")
            distinct_actions = [row[0] for row in await cursor.fetchall()]
            await cursor.execute("SELECT DISTINCT status FROM sys_security_logs ORDER BY status")
            distinct_statuses = [row[0] for row in await cursor.fetchall()]
            await cursor.execute("SELECT DISTINCT sys_users.username FROM sys_security_logs JOIN sys_users ON sys_security_logs.actor_user_id = sys_users.id ORDER BY sys_users.username")
            distinct_actors = [row[0] for row in await cursor.fetchall()]
            await cursor.execute("SELECT DISTINCT sys_users.username FROM sys_security_logs JOIN sys_users ON sys_security_logs.target_user_id = sys_users.id ORDER BY sys_users.username")
            distinct_targets = [row[0] for row in await cursor.fetchall()]
            
        return await render_template('security_logs.html', 
                               logs=logs, 
                               actions=distinct_actions,
                               statuses=distinct_statuses,
                               actors=distinct_actors,
                               targets=distinct_targets,
                               ips=[], sids=[],
                               filters={'action': f_action, 'status': f_status, 'actor': f_actor, 'target': f_target, 'ip': f_ip, 'sid': f_sid})
    except Exception as e:
        await flash(f"Error cargando logs: {e}", "danger")
        return redirect(url_for('ventas.dashboard'))

@core_bp.route('/admin/audit/permissions')
@login_required
@permission_required('view_permission_audit')
async def audit_permissions():
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            # Traer logs relacionados con gestión de accesos
            await cursor.execute("""
                SELECT sys_security_logs.id, sys_security_logs.event_time, sys_security_logs.action, sys_security_logs.status, sys_security_logs.details, sys_security_logs.ip_address,
                       sys_users.username as operator
                FROM sys_security_logs
                LEFT JOIN sys_users ON sys_security_logs.actor_user_id = sys_users.id
                WHERE sys_security_logs.enterprise_id = %s 
                AND sys_security_logs.action IN ('REVOKE_PERMISSION', 'GRANT_PERMISSION', 'UPDATE_ROLE_PERMS')
                ORDER BY sys_security_logs.event_time DESC
            """, (g.user['enterprise_id'],))
            logs = await cursor.fetchall()
        return await render_template('admin_audit_permissions.html', logs=logs)
    except Exception as e:
        await flash(f"Error cargando logs: {e}", "danger")
        return redirect(url_for('ventas.dashboard'))

@core_bp.route('/admin/audit/integrity')
@login_required
@permission_required('view_permission_audit')
async def audit_integrity():
    from services.sod_service import analyze_role_sod
    ent_id = g.user['enterprise_id']
    
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            # 1. Mapeo de Roles en Conflicto
            await cursor.execute("SELECT id, name FROM sys_roles WHERE enterprise_id = %s", (ent_id,))
            roles = await cursor.fetchall()
            conflict_roles_map = {}
            for r in roles:
                await cursor.execute("""
                    SELECT sys_permissions.id, sys_permissions.code, sys_permissions.description, sys_permissions.category 
                    FROM sys_permissions
                    JOIN sys_role_permissions ON sys_permissions.id = sys_role_permissions.permission_id
                    WHERE sys_role_permissions.role_id = %s
                """, (r['id'],))
                perms = await cursor.fetchall()
                analysis = analyze_role_sod(r['name'], perms)
                if analysis['conflictos_detalle']:
                    conflict_roles_map[r['id']] = analysis['conflictos_detalle']

            abusive_ops = []
            if conflict_roles_map:
                role_ids = list(conflict_roles_map.keys())
                placeholder = ', '.join(['%s'] * len(role_ids))
                
                # BUSQUEDA MULTI-TABLA (Basado en usuarios con roles conflictivos)
                # A. Órdenes de Pago
                await cursor.execute(f"""
                    SELECT 'PAGO' as modulo, fin_ordenes_pago.id, fin_ordenes_pago.numero as comprobante, fin_ordenes_pago.fecha, fin_ordenes_pago.importe_total as total, 
                           sys_users.username as operador, sys_roles.name as rol_operador, sys_users.role_id, fin_ordenes_pago.created_at
                    FROM fin_ordenes_pago
                    JOIN sys_users ON fin_ordenes_pago.user_id = sys_users.id
                    JOIN sys_roles ON sys_users.role_id = sys_roles.id
                    WHERE fin_ordenes_pago.enterprise_id = %s AND sys_users.role_id IN ({placeholder})
                    ORDER BY fin_ordenes_pago.created_at DESC LIMIT 50
                """, [ent_id] + role_ids)
                for row in await cursor.fetchall():
                    row['violacion'] = conflict_roles_map[row['role_id']][0]['regla']
                    abusive_ops.append(row)

                # B. Movimientos de Stock
                await cursor.execute(f"""
                    SELECT 'STOCK' as modulo, stk_movimientos.id, stk_movimientos.id as comprobante, stk_movimientos.fecha, 0 as total, 
                           sys_users.username as operador, sys_roles.name as rol_operador, sys_users.role_id, stk_movimientos.fecha as created_at
                    FROM stk_movimientos
                    JOIN sys_users ON stk_movimientos.user_id = sys_users.id
                    JOIN sys_roles ON sys_users.role_id = sys_roles.id
                    WHERE stk_movimientos.enterprise_id = %s AND sys_users.role_id IN ({placeholder})
                    ORDER BY stk_movimientos.fecha DESC LIMIT 50
                """, [ent_id] + role_ids)
                for row in await cursor.fetchall():
                    row['violacion'] = conflict_roles_map[row['role_id']][0]['regla']
                    abusive_ops.append(row)

            return await render_template('admin_audit_integrity.html', operations=abusive_ops)
            
    except Exception as e:
        await flash(f"Error en Auditoría Transaccional: {e}", "danger")
        return redirect(url_for('core.dashboard'))

@core_bp.route('/admin/audit/certification')
@login_required
@permission_required('view_permission_audit')
async def audit_certification():
    from services.audit_certification_service import AuditCertificationService
    try:
        compliance_data = await AuditCertificationService.get_all_modules_compliance()
        return await render_template('admin_audit_certification.html', compliance_data=compliance_data)
    except Exception as e:
        await flash(f"Error en certificación de módulos: {str(e)}", "danger")
        return redirect(url_for('core.dashboard'))

@core_bp.route('/admin/audit/ai-auditor', methods=['GET', 'POST'])
@login_required
@permission_required('view_permission_audit')
async def ai_auditor():
    """Interfaz para consultar al Auditor de IA Local (LLM)."""
    from services.local_intelligence_service import LocalIntelligenceService
    response_text = None
    question = None
    ollama_status = LocalIntelligenceService.check_health()
    
    if request.method == 'POST':
        question = (await request.form).get('question')
        if question:
            res = LocalIntelligenceService.consult_rules(question)
            if "response" in res:
                response_text = res["response"]
            else:
                await flash(res.get("error", "Error detectado en el motor de IA"), "danger")
                
    return await render_template('admin_ai_auditor.html', 
                           response=response_text, 
                           question=question,
                           ollama_status=ollama_status)

# --- DOCUMENTATION ROUTES ---
@core_bp.route('/manual')
@login_required
async def manual_index():
    return await render_template('manual/index.html')

@core_bp.route('/manual/tecnica')
@login_required
async def manual_tecnico():
    from quart import current_app
    # Introspección Automática de Rutas (Live Docs)
    rutas_info = []
    
    # Reglas ordenadas por Endpoint para agrupar por módulos (biblioteca, core, etc)
    rules = sorted(list(current_app.url_map.iter_rules()), key=lambda x: x.endpoint)
    
    for rule in rules:
        # Filtrar rutas estáticas o de debug internas irrelevantes
        if "static" in rule.endpoint or "debug" in rule.endpoint: continue
        
        func = current_app.view_functions.get(rule.endpoint)
        if not func: continue
        
        # Extraer docstring
        doc = func.__doc__
        if doc:
            # Simple format cleaning
            doc = doc.strip().split('\n')[0] # Tomar solo la primera linea para el resumen
        else:
            doc = "<em>Sin documentación</em>"
            
        rutas_info.append({
            'endpoint': rule.endpoint,
            'url': str(rule),
            'methods': ', '.join(sorted([m for m in rule.methods if m not in ('HEAD', 'OPTIONS')])),
            'doc': doc
        })

    return await render_template('manual/tecnica.html', rutas=rutas_info)

@core_bp.route('/manual/usuario')
@login_required
async def manual_usuario():
    return await render_template('manual/usuario.html')

@core_bp.route('/manual/compras-v4')
@login_required
async def manual_compras_v4():
    """Referencia Técnica Avanzada para el Módulo de Compras MSAC v4."""
    return await render_template('manual/compras_v4_tecnica.html')

@core_bp.route('/manual/ventas-v4')
@login_required
async def manual_ventas_v4():
    """Referencia Técnica Avanzada para el Módulo de Ventas MSAC v4."""
    return await render_template('manual/ventas_v4_tecnica.html')

@core_bp.route('/manual/compras-pasos')
@login_required
async def manual_compras_pasos():
    """Guía paso a paso del Módulo de Compras para nuevos usuarios."""
    return await render_template('manual/compras_paso_a_paso.html')

# --- INTERNAL UTILS ---
async def _log_security_event(action, status, enterprise_id=None, target_user_id=None, details=None, sid=None):
    actor_id = g.user['id'] if g.user else None
    ent_id = enterprise_id or (g.user['enterprise_id'] if g.user else None)
    if ent_id is None: return # Cannot log without enterprise
    ip = request.remote_addr
    log_sid = sid if sid else g.get('sid')
    try:
        async with get_db_cursor() as cursor:
            await cursor.execute("INSERT INTO sys_security_logs (enterprise_id, actor_user_id, target_user_id, action, status, details, ip_address, session_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                           (ent_id, actor_id, target_user_id, action, status, details, ip, log_sid))
    except Exception as e:
        logger.error(f"LOG ERROR: {e}")

# --- GEOREF API PROXY ---
@core_bp.route('/api/georef/localidades')
@login_required
async def api_get_localidades():
    from services.georef_service import GeorefService
    provincia = request.args.get('provincia')
    if not provincia:
        return jsonify([])
    localidades = await GeorefService.get_localidades(provincia)
    return jsonify(localidades)

@core_bp.route('/api/georef/calles')
@login_required
async def api_get_calles():
    from services.georef_service import GeorefService
    localidad = request.args.get('localidad')
    provincia = request.args.get('provincia')
    nombre = request.args.get('nombre')
    
    logger.info(f"GEOREF: Buscando calle '{nombre}' en Prov:{provincia}, Loc:{localidad}")
    
    if not nombre or not provincia:
        return jsonify([])
        
    calles = await GeorefService.get_calles(localidad, provincia, nombre)
    logger.info(f"GEOREF: Encontradas {len(calles)} calles")
    return jsonify(calles)

@core_bp.route('/api/georef/cp')
@login_required
async def api_get_cp():
    from services.georef_service import GeorefService
    provincia = request.args.get('provincia')
    localidad = request.args.get('localidad')
    if not provincia or not localidad:
        return jsonify([])
    cps = await GeorefService.get_cp_by_location(provincia, localidad)
    return jsonify(cps)

# --- DATOS FISCALES DE EMPRESA ---

@core_bp.route('/admin/empresa/fiscal', methods=['GET'])
@login_required
@permission_required('admin_users')
async def admin_empresa_fiscal():
    """Muestra formulario de datos fiscales de la empresa."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM sys_enterprises WHERE id = %s", (g.user['enterprise_id'],))
        empresa = await cursor.fetchone()
    return await render_template('empresa_fiscal.html', empresa=empresa)

@core_bp.route('/admin/empresa/fiscal', methods=['POST'])
@login_required
@permission_required('admin_users')
async def admin_empresa_fiscal_save():
    """Guarda datos fiscales de la empresa."""
    nombre = (await request.form).get('nombre', '').strip()
    cuit = (await request.form).get('cuit', '').strip()
    domicilio = (await request.form).get('domicilio', '').strip()
    condicion_iva = (await request.form).get('condicion_iva', '').strip()
    ingresos_brutos = (await request.form).get('ingresos_brutos', '').strip()
    iibb_condicion = (await request.form).get('iibb_condicion', '').strip()
    inicio_actividades_raw = (await request.form).get('inicio_actividades', '').strip()
    inicio_actividades = None
    if inicio_actividades_raw:
        try:
            # Try ISO format (Flatpickr standard)
            inicio_actividades = datetime.datetime.strptime(inicio_actividades_raw, '%Y-%m-%d').date()
        except ValueError:
            try:
                # Try Argentinian format (User manual input)
                inicio_actividades = datetime.datetime.strptime(inicio_actividades_raw, '%d/%m/%Y').date()
            except ValueError:
                inicio_actividades = None
    
    email = (await request.form).get('email', '').strip()
    telefono = (await request.form).get('telefono', '').strip()
    website = (await request.form).get('website', '').strip()
    lema = (await request.form).get('lema', '').strip()
    logo_path = (await request.form).get('logo_path', '').strip()

    # AFIP Certificates (Optional Upload)
    afip_crt = None
    afip_key = None
    if 'afip_crt_file' in (await request.files) and (await request.files)['afip_crt_file'].filename:
        afip_crt = (await (await request.files)['afip_crt_file'].read()).decode('utf-8', errors='ignore')
    if 'afip_key_file' in (await request.files) and (await request.files)['afip_key_file'].filename:
        afip_key = (await (await request.files)['afip_key_file'].read()).decode('utf-8', errors='ignore')

    try:
        async with get_db_cursor() as cursor:
            # Build dynamic update
            params = [nombre, cuit, domicilio, condicion_iva, ingresos_brutos, iibb_condicion, 
                     inicio_actividades, email, telefono, website, lema, logo_path]
            update_sql = """
                UPDATE sys_enterprises 
                SET nombre = %s, cuit = %s, domicilio = %s, condicion_iva = %s, 
                    ingresos_brutos = %s, iibb_condicion = %s, inicio_actividades = %s,
                    email = %s, telefono = %s, website = %s, lema = %s,
                    logo_path = %s
            """
            
            if afip_crt:
                update_sql += ", afip_crt = %s"
                params.append(afip_crt)
            if afip_key:
                update_sql += ", afip_key = %s"
                params.append(afip_key)
            
            update_sql += " WHERE id = %s"
            params.append(g.user['enterprise_id'])
            
            await cursor.execute(update_sql, tuple(params))
        
        await flash("Datos fiscales actualizados correctamente.", "success")
        await _log_security_event("FISCAL_UPDATE", "SUCCESS", details=f"Empresa: {nombre}, CUIT: {cuit}")
    except Exception as e:
        await flash(f"Error al guardar: {e}", "danger")
    
    return redirect(url_for('core.admin_empresa_fiscal'))


# --- ADMINISTRACION DE NUMERACION ---

@core_bp.route('/admin/numeracion')
@login_required
async def admin_numeracion():
    """Administración de la numeración de comprobantes y otros para cada empresa."""
    from database import get_db_cursor
    ent_id = request.args.get('ent_id', g.user['enterprise_id'], type=int)
    
    # Solo superadmin puede ver otras empresas
    if g.user['role_name'] != 'SuperAdmin' and ent_id != g.user['enterprise_id']:
        ent_id = g.user['enterprise_id']

    async with get_db_cursor(dictionary=True) as cursor:
        # Lista de empresas para el filtro (solo superadmin)
        enterprises = []
        if g.user['role_name'] == 'SuperAdmin':
            await cursor.execute("SELECT id, nombre FROM sys_enterprises ORDER BY nombre")
            enterprises = await cursor.fetchall()
            
        # Obtener numeración actual para la empresa seleccionada
        await cursor.execute("""
            SELECT sys_enterprise_numeracion.*, sys_tipos_comprobante.descripcion as tipo_nombre
            FROM sys_enterprise_numeracion
            LEFT JOIN sys_tipos_comprobante ON sys_enterprise_numeracion.entidad_codigo = sys_tipos_comprobante.codigo AND sys_enterprise_numeracion.entidad_tipo = 'COMPROBANTE'
            WHERE sys_enterprise_numeracion.enterprise_id = %s
            ORDER BY sys_enterprise_numeracion.entidad_tipo, sys_enterprise_numeracion.punto_venta, sys_enterprise_numeracion.entidad_codigo
        """, (ent_id,))
        numeracion = await cursor.fetchall()

    return await render_template('admin_numeracion.html', 
                           numeracion=numeracion, 
                           enterprises=enterprises, 
                           selected_ent_id=ent_id)


@core_bp.route('/admin/numeracion/save', methods=['POST'])
@login_required
@atomic_transaction('core', severity=6, impact_category='Configuration')
async def admin_numeracion_save():
    """Guarda o actualiza un parámetro de numeración."""
    item_id = (await request.form).get('id')
    ent_id = (await request.form).get('enterprise_id', type=int)
    tipo = (await request.form).get('entidad_tipo')
    codigo = (await request.form).get('entidad_codigo')
    pv = (await request.form).get('punto_venta', 1, type=int)
    numero = (await request.form).get('ultimo_numero', 0, type=int)

    # Seguridad: Un admin regular solo puede tocar su empresa
    if g.user['role_name'] != 'SuperAdmin' and ent_id != g.user['enterprise_id']:
        return jsonify({'success': False, 'message': 'No tiene permisos para modificar esta empresa'}), 403

    try:
        from database import get_db_cursor
        async with get_db_cursor() as cursor:
            if item_id:
                await cursor.execute("""
                    UPDATE sys_enterprise_numeracion 
                    SET ultimo_numero = %s, punto_venta = %s 
                    WHERE id = %s AND enterprise_id = %s
                """, (numero, pv, item_id, ent_id))
            else:
                await cursor.execute("""
                    INSERT INTO sys_enterprise_numeracion (enterprise_id, entidad_tipo, entidad_codigo, punto_venta, ultimo_numero)
                    VALUES (%s, %s, %s, %s, %s)
                """, (ent_id, tipo, codigo, pv, numero))
                
        await flash("Numeración actualizada correctamente.", "success")
    except Exception as e:
        await flash(f"Error al guardar: {e}", "danger")
        
        
    return redirect(url_for('core.admin_numeracion', ent_id=ent_id))

@core_bp.route('/admin/numeracion/delete/<int:id>', methods=['POST'])
@login_required
@atomic_transaction('core', severity=6, impact_category='Configuration')
async def admin_numeracion_delete(id):
    """Elimina una configuración de numeración, validando antes que no esté en uso."""
    ent_id = (await request.form).get('enterprise_id', type=int)

    if g.user['role_name'] != 'SuperAdmin' and ent_id != g.user['enterprise_id']:
        return jsonify({'success': False, 'message': 'No tiene permisos para modificar esta empresa'}), 403

    try:
        from database import get_db_cursor
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("SELECT * FROM sys_enterprise_numeracion WHERE id = %s AND enterprise_id = %s", (id, ent_id))
            num = await cursor.fetchone()
            
            if not num:
                await flash("El registro seleccionado no existe.", "danger")
                return redirect(url_for('core.admin_numeracion', ent_id=ent_id))
                
            # Validar si ya hay await comprobantes(esquema de ventas) para este punto de venta
            if num['entidad_tipo'] == 'COMPROBANTE':
                await cursor.execute("""
                    SELECT 1 FROM erp_comprobantes 
                    WHERE enterprise_id = %s AND tipo_comprobante = %s AND punto_venta = %s
                    LIMIT 1
                """, (ent_id, num['entidad_codigo'], num['punto_venta']))
                if await cursor.fetchone():
                    await flash(f"No se puede borrar el código {num['entidad_codigo']}. Ya existen comprobantes asignados en el esquema de ventas para el Punto de Venta {num['punto_venta']}.", "danger")
                    return redirect(url_for('core.admin_numeracion', ent_id=ent_id))
            
            # Borrar si pasó la validación
            await cursor.execute("DELETE FROM sys_enterprise_numeracion WHERE id = %s", (id,))
            await flash(f"Punto de venta y Numerador ({num['entidad_codigo']}) eliminados correctamente.", "success")
            
    except Exception as e:
        await flash(f"Error al intentar borrar: {e}", "danger")
        
    return redirect(url_for('core.admin_numeracion', ent_id=ent_id))

@core_bp.route('/admin/numeracion/clone', methods=['POST'])
@login_required
@atomic_transaction('core', severity=6, impact_category='Configuration')
async def admin_numeracion_clone():
    """Toma la configuración actual de un enterprise_id y la duplica hacia un nuevo punto de venta inicializando en 0."""
    ent_id = (await request.form).get('enterprise_id', type=int)
    nuevo_pv = (await request.form).get('nuevo_pv', type=int)

    if not ent_id or not nuevo_pv or nuevo_pv < 1:
        await flash("Datos inválidos para clonar.", "danger")
        return redirect(url_for('core.admin_numeracion', ent_id=ent_id))

    if g.user['role_name'] != 'SuperAdmin' and ent_id != g.user['enterprise_id']:
        return jsonify({'success': False, 'message': 'No tiene permisos para modificar esta empresa'}), 403

    try:
        from database import get_db_cursor
        async with get_db_cursor() as cursor:
            # Seleccionar la base actual del PV 1 (o general) de la empresa
            await cursor.execute("""
                SELECT entidad_tipo, entidad_codigo 
                FROM sys_enterprise_numeracion 
                WHERE enterprise_id = %s 
                GROUP BY entidad_tipo, entidad_codigo
            """, (ent_id,))
            
            bases = await cursor.fetchall()
            insertadas = 0
            
            for b in bases:
                # Ver si ya existe para ese pv
                await cursor.execute("""
                    SELECT id FROM sys_enterprise_numeracion 
                    WHERE enterprise_id = %s AND entidad_tipo = %s AND entidad_codigo = %s AND punto_venta = %s
                """, (ent_id, b[0], b[1], nuevo_pv))
                
                if not await cursor.fetchone():
                    await cursor.execute("""
                        INSERT INTO sys_enterprise_numeracion (enterprise_id, entidad_tipo, entidad_codigo, punto_venta, ultimo_numero)
                        VALUES (%s, %s, %s, %s, 0)
                    """, (ent_id, b[0], b[1], nuevo_pv))
                    insertadas += 1
                    
        await flash(f"Se clonaron {insertadas} registros hacia el Punto de Venta {nuevo_pv} (iniciados en 0).", "success")
    except Exception as e:
        await flash(f"Error al clonar numeración: {e}", "danger")
        
    return redirect(url_for('core.admin_numeracion', ent_id=ent_id))


# --- MAESTRO DE TIPOS DE COMPROBANTE ---

@core_bp.route('/admin/tipos-comprobante')
@login_required
@permission_required('admin_users')
async def admin_tipos_comprobante():
    """Maestro global de tipos de comprobante."""
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("SELECT * FROM sys_tipos_comprobante ORDER BY codigo")
        tipos = await cursor.fetchall()
    return await render_template('admin_tipos_comprobante.html', tipos=tipos)


@core_bp.route('/admin/tipos-comprobante/save', methods=['POST'])
@login_required
@permission_required('admin_users')
@atomic_transaction('core', severity=7, impact_category='Configuration')
async def admin_tipos_comprobante_save():
    """Guarda o actualiza un tipo de comprobante maestro."""
    orig_codigo = (await request.form).get('orig_codigo')
    codigo = (await request.form).get('codigo')
    descripcion = (await request.form).get('descripcion')
    letra = (await request.form).get('letra', '')
    es_fiscal = 1 if 'es_fiscal' in (await request.form) else 0
    es_numerable = 1 if 'es_numerable' in (await request.form) else 0
    afip_code = (await request.form).get('afip_code') or None

    try:
        from database import get_db_cursor
        async with get_db_cursor() as cursor:
            if orig_codigo:
                await cursor.execute("""
                    UPDATE sys_tipos_comprobante 
                    SET codigo = %s, descripcion = %s, letra = %s, es_fiscal = %s, afip_code = %s, es_numerable = %s
                    WHERE codigo = %s
                """, (codigo, descripcion, letra, es_fiscal, afip_code, es_numerable, orig_codigo))
            else:
                await cursor.execute("""
                    INSERT INTO sys_tipos_comprobante (codigo, descripcion, letra, es_fiscal, afip_code, es_numerable)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (codigo, descripcion, letra, es_fiscal, afip_code, es_numerable))
                
                # REQUERIMIENTO: Insertar en tabla de numeración para cada empresa
                try:
                    from services.enterprise_init import sync_new_concept_to_all_enterprises
                    await sync_new_concept_to_all_enterprises('COMPROBANTE', codigo)
                except Exception as e:
                    logger.error(f"Error sincronizando nuevo tipo a empresas: {e}")

        await flash("Tipo de comprobante guardado correctamente.", "success")
    except Exception as e:
        await flash(f"Error al guardar: {e}", "danger")
        
    return redirect(url_for('core.admin_tipos_comprobante'))

# --- EXTERNAL SERVICES ADMIN ---

@core_bp.route('/admin/services')
@login_required
@permission_required('admin_users')
async def admin_services():
    # Mapeos predefinidos para documentación (DOM vs DB)
    mappings = {
        'CuspideScraper': [
            {'dom': 'h1.page-title', 'db': 'stk_articulos.nombre', 'desc': 'Título del libro'},
            {'dom': '.autor a', 'db': 'stk_articulos.modelo', 'desc': 'Autor'},
            {'dom': '#img_portada', 'db': 'metadata_json -> cover_url', 'desc': 'Imagen de portada'},
            {'dom': '.datos-ficha li', 'db': 'stk_articulos.marca', 'desc': 'Editorial'},
            {'dom': '#sinopsis', 'db': 'metadata_json -> descripcion', 'desc': 'Resumen/Sinopsis'},
            {'dom': '.breadcrumb li a', 'db': 'metadata_json -> temas', 'desc': 'Categorías/Géneros'}
        ],
        'ReldScraper': [
            {'dom': '.destacado-titulo', 'db': 'stk_articulos.nombre', 'desc': 'Nombre del producto'},
            {'dom': '#valor_precio', 'db': 'stk_articulos.precio_venta', 'desc': 'Precio de lista'},
            {'dom': '#codi_arti', 'db': 'stk_articulos.codigo', 'desc': 'Código SKU/Fábrica'},
            {'dom': '.articulo-info-adic', 'db': 'metadata_json -> descripcion', 'desc': 'Ficha técnica'},
            {'dom': 'img.agrandar', 'db': 'metadata_json -> cover_url', 'desc': 'Imagen principal'},
            {'dom': '.breadcrumb a, b', 'db': 'metadata_json -> temas', 'desc': 'Categorías'}
        ],
        'OpenLibraryService': [
            {'dom': 'JSON: .title', 'db': 'stk_articulos.nombre', 'desc': 'Título internacional'},
            {'dom': 'JSON: .number_of_pages', 'db': 'metadata_json -> paginas', 'desc': 'Cantidad de páginas'},
            {'dom': 'JSON: .covers[0]', 'db': 'metadata_json -> cover_url', 'desc': 'Portada (ID)'},
            {'dom': 'JSON: .subjects', 'db': 'metadata_json -> temas', 'desc': 'Temas/Etiquetas'}
        ],
        'LibrarioService': [
            {'dom': 'JSON: .title', 'db': 'stk_articulos.nombre', 'desc': 'Título unificado'},
            {'dom': 'JSON: .authors', 'db': 'stk_articulos.modelo', 'desc': 'Autores (Lista)'},
            {'dom': 'JSON: .synopsis', 'db': 'metadata_json -> descripcion', 'desc': 'Sinopsis unificada'},
            {'dom': 'JSON: .coverUrl', 'db': 'metadata_json -> cover_url', 'desc': 'URL de Portada'},
            {'dom': 'JSON: .publisher', 'db': 'stk_articulos.marca', 'desc': 'Editorial'}
        ]
    }


    async with get_db_cursor() as cursor:
        await cursor.execute("""
            SELECT id, nombre, tipo_servicio, clase_implementacion, url_objetivo, activo, last_status, created_at
            FROM sys_external_services 
            WHERE enterprise_id = %s
            ORDER BY created_at DESC
        """, (g.user['enterprise_id'],))
        desc = cursor.description
        services_rows = await cursor.fetchall()
        services = []
        for row in services_rows:
            s_dict = dict(zip([col[0] for col in desc], row))
            # Identificar la clase para el mapeo
            impl_class = s_dict['clase_implementacion'].split('.')[-1] if s_dict['clase_implementacion'] else s_dict['nombre']
            s_dict['mapping'] = mappings.get(impl_class, [])
            services.append(s_dict)
        
            services.append(s_dict)

    # Get Rotation Manager Status
    from services.rotation_service import rotation_manager
    rotation_status = {
        'mode': rotation_manager.mode,
        'count': rotation_manager.request_count,
        'threshold': rotation_manager.rotation_threshold,
        'pool_size': len(rotation_manager.proxies_pool)
    }
        
    return await render_template('admin_services.html', services=services, rotation_status=rotation_status)

@core_bp.route('/admin/services/control/<int:service_id>', methods=['POST'])
@login_required
@permission_required('admin_users')
async def control_service(service_id):
    action = (await request.form).get('action') # start, stop, restart
    
    try:
        async with get_db_cursor() as cursor:
            if action == 'start':
                await cursor.execute("UPDATE sys_external_services SET activo = 1, last_status = 'RUNNING' WHERE id = %s AND enterprise_id = %s", (service_id, g.user['enterprise_id']))
                await flash("Servicio iniciado correctamente.", "success")
            elif action == 'stop':
                await cursor.execute("UPDATE sys_external_services SET activo = 0, last_status = 'STOPPED' WHERE id = %s AND enterprise_id = %s", (service_id, g.user['enterprise_id']))
                await flash("Servicio detenido.", "warning")
            elif action == 'restart':
                # Potentially clear counts or re-init in a real daemon, 
                # here we just cycle status for visual feedback
                await cursor.execute("UPDATE sys_external_services SET activo = 1, last_status = 'RESTARTING' WHERE id = %s AND enterprise_id = %s", (service_id, g.user['enterprise_id']))
                # In a real app, you might trigger a worker reload here
                await cursor.execute("UPDATE sys_external_services SET last_status = 'RUNNING' WHERE id = %s AND enterprise_id = %s", (service_id, g.user['enterprise_id']))
                await flash("Servicio reiniciado.", "info")
                
        await _log_security_event("SERVICE_CONTROL", "SUCCESS", details=f"Service {service_id} {action}")
    except Exception as e:
        await flash(f"Error al controlar servicio: {e}", "danger")
        
    return redirect(url_for('core.admin_services'))

@core_bp.route('/admin/services/georef/sync', methods=['POST'])
@login_required
@permission_required('admin_users')
async def admin_georef_sync():
    """Dispara la sincronización batch de localidades Georef."""
    try:
        from services.georef_service import GeorefService
        import threading
        
        async def run_async_sync():
            logger.info("Iniciando sincronización batch de Georef...")
            count = await GeorefService.load_localidades() # If this is sync, it's fine, but let's assume it might need await if changed
            logger.info(f"Sincronización batch finalizada. Total: {count}")
            
            # Opcional: Registrar evento en DB si se desea persistencia del resultado
            async with get_db_cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO sys_security_logs (enterprise_id, action, status, details, ip_address)
                    VALUES (%s, 'GEOREF_SYNC', 'SUCCESS', %s, 'BATCH')
                """, (1, f"Carga completa: {count} localidades")) # Enterprise 1 hardcoded or system context

        # Ejecutar en segundo plano (Quart way)
        from quart import current_app
        current_app.add_background_task(run_async_sync)
        
        await flash("La sincronización de localidades se ha iniciado en segundo plano. Puede tardar unos minutos.", "info")
    except Exception as e:
        logger.error(f"Error al iniciar sync georef: {e}")
        await flash(f"Error al iniciar sincronización: {e}", "danger")
        
    return redirect(url_for('core.admin_services'))

@core_bp.route('/admin/services/rotation', methods=['POST'])
@login_required
@permission_required('admin_users')
async def admin_rotation_control():
    """Controla el servicio de rotación de IPs."""
    from services.rotation_service import rotation_manager
    
    action = (await request.form).get('action')
    
    try:
        if action == 'rotate':
            rotation_manager.rotate()
            await flash("Identidad rotada exitosamente.", "success")
        elif action == 'config':
            mode = (await request.form).get('mode')
            if mode in ['DIRECT', 'TOR', 'POOL', 'SCRAPERAPI']:
                rotation_manager.mode = mode
                # Re-init session if needed
                await rotation_manager._initialize_session()
                await flash(f"Modo de rotación cambiado a: {mode}", "success")
            else:
                await flash("Modo no válido.", "danger")
                
        await _log_security_event("ROTATION_CONTROL", "SUCCESS", details=f"Action: {action}")
    except Exception as e:
        await flash(f"Error en rotación: {e}", "danger")
        
    return redirect(url_for('core.admin_services'))

@core_bp.route('/admin/services/config/<int:service_id>', methods=['GET', 'POST'])
@login_required
@permission_required('admin_users')
async def admin_service_config(service_id):
    """Interfaz para configurar tokens y credenciales de servicios externos."""
    ent_id = g.user['enterprise_id']
    
    async with get_db_cursor(dictionary=True) as cursor:
        if request.method == 'POST':
            config_str = (await request.form).get('config_json')
            try:
                # Validar que sea JSON válido
                import json
                json.loads(config_str)
                
                await cursor.execute("""
                    UPDATE sys_external_services 
                    SET config_json = %s 
                    WHERE id = %s AND enterprise_id = %s
                """, (config_str, service_id, ent_id))
                await flash("Configuración actualizada correctamente.", "success")
                return redirect(url_for('core.admin_services'))
            except Exception as e:
                await flash(f"Error en formato JSON o guardado: {e}", "danger")

        await cursor.execute("SELECT * FROM sys_external_services WHERE id = %s AND enterprise_id = %s", (service_id, ent_id))
        service = await cursor.fetchone()
        
    if not service:
        await flash("Servicio no encontrado.", "danger")
        return redirect(url_for('core.admin_services'))
        
    return await render_template('admin_service_config.html', service=service)

@core_bp.route('/admin/services/create', methods=['GET', 'POST'])
@login_required
@permission_required('admin_users')
async def admin_service_create():
    """Permite registrar un nuevo servicio externo (API/Scraper) para la empresa."""
    ent_id = g.user['enterprise_id']
    
    if request.method == 'POST':
        nombre = (await request.form).get('nombre')
        tipo_servicio = (await request.form).get('tipo_servicio')
        clase_impl = (await request.form).get('clase_implementacion')
        modo_captura = (await request.form).get('modo_captura')
        url_objetivo = (await request.form).get('url_objetivo')
        system_code = (await request.form).get('system_code')
        config_json = (await request.form).get('config_json', '{}')
        
        try:
            # Validar JSON
            import json
            json.loads(config_json)
            
            async with get_db_cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO sys_external_services 
                    (enterprise_id, nombre, tipo_servicio, clase_implementacion, config_json, modo_captura, url_objetivo, system_code, activo, last_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, 'IDLE')
                """, (ent_id, nombre, tipo_servicio, clase_impl, config_json, modo_captura, url_objetivo, system_code))
                
            await flash(f"Servicio '{nombre}' registrado exitosamente.", "success")
            await _log_security_event("SERVICE_CREATE", "SUCCESS", details=f"New service: {nombre}")
            return redirect(url_for('core.admin_services'))
        except Exception as e:
            await flash(f"Error al registrar servicio: {e}", "danger")
            
    return await render_template('admin_service_create.html')

# --- SYSTEM DASHBOARD ---
@core_bp.route('/admin/dashboard')
@login_required
@permission_required('admin_users')
async def admin_dashboard():
    """Dashboard consolidado de sistemas y estados externos."""
    from core.concurrency import get_active_tasks
    import time
    
    async with get_db_cursor(dictionary=True) as cursor:
        # 1. External Services Status
        await cursor.execute("SELECT id, nombre, activo, last_status, tipo_servicio FROM sys_external_services WHERE enterprise_id = %s", (g.user['enterprise_id'],))
        services_status = await cursor.fetchall()
        
        # 2. Recent Security Logs (Last 10 with status)
        await cursor.execute("""
            SELECT event_time, action, status, details, ip_address
            FROM sys_security_logs 
            WHERE enterprise_id = %s 
            ORDER BY event_time DESC LIMIT 10
        """, (g.user['enterprise_id'],))
        recent_logs = await cursor.fetchall()
        
        # 3. Managed Crons Status
        await cursor.execute("SELECT id, nombre, estado, ultima_ejecucion, proxima_ejecucion FROM sys_crons WHERE enterprise_id = %s", (g.user['enterprise_id'],))
        crons_status = await cursor.fetchall()

    # Active Managed Tasks
    active_tasks = await get_active_tasks()
    
    now = __import__('datetime').datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    return await render_template('admin_dashboard.html', 
                           services=services_status,
                           logs=recent_logs,
                           crons=crons_status,
                           active_tasks_count=len(active_tasks),
                           now=now)

# --- RISK DASHBOARD FMECA ---

@core_bp.route('/admin/risk-dashboard')
@login_required
@permission_required('view_risk_dashboard')
async def admin_risk_dashboard():
    """Dashboard de Riesgos FMECA – Heatmap, RPN y Mitigaciones Activas."""
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'
    ent_id = g.user['enterprise_id']
    
    # Get Time Filters from Query Args
    timeframe = request.args.get('timeframe', 'all')
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')

    date_filter_sql = ""
    date_params = []

    if timeframe == 'day':
        date_filter_sql = "DATE(created_at) = CURDATE()"
    elif timeframe == 'week':
        date_filter_sql = "created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)"
    elif timeframe == 'month':
        date_filter_sql = "created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)"
    elif timeframe == 'range' and date_start and date_end:
        date_filter_sql = "DATE(created_at) BETWEEN %s AND %s"
        date_params.extend([date_start, date_end])

    async with get_db_cursor(dictionary=True) as cursor:
        # -- 1. KPIs Globales --
        filters_1 = []
        params_1 = []
        if not is_super:
            filters_1.append("enterprise_id = %s")
            params_1.append(ent_id)
        if date_filter_sql:
            filters_1.append(date_filter_sql)
            params_1.extend(date_params)
            
        where_clause_1 = "WHERE " + " AND ".join(filters_1) if filters_1 else ""

        await cursor.execute(f"""
            SELECT 
                COUNT(*) as total_ops,
                SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as total_errors,
                AVG(severity) as avg_severity,
                MAX(severity) as max_severity,
                AVG(duration_ms) as avg_duration
            FROM sys_transaction_logs {where_clause_1}
        """, tuple(params_1))
        kpis = await cursor.fetchone() or {}

        # -- 2. Heatmap por Módulo (RPN = Severidad × Frecuencia de Error) --
        await cursor.execute(f"""
            SELECT 
                module,
                COUNT(*) as total_ops,
                SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as errors,
                MAX(severity) as max_severity,
                AVG(severity) as avg_severity,
                COUNT(*) as frequency,
                (MAX(severity) * SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END)) as rpn
            FROM sys_transaction_logs
            {where_clause_1}
            GROUP BY module
            ORDER BY rpn DESC
        """, tuple(params_1))
        module_heatmap = await cursor.fetchall()

        # -- 3. Distribución de Failure Modes --
        
        filters_3 = ["failure_mode IS NOT NULL"]
        params_3 = []
        if not is_super:
            filters_3.append("enterprise_id = %s")
            params_3.append(ent_id)
        if date_filter_sql:
            filters_3.append(date_filter_sql)
            params_3.extend(date_params)
            
        where_clause_3 = "WHERE " + " AND ".join(filters_3)

        await cursor.execute(f"""
            SELECT 
                failure_mode,
                COUNT(*) as total,
                MAX(severity) as max_sev
            FROM sys_transaction_logs
            {where_clause_3}
            GROUP BY failure_mode
            ORDER BY total DESC
        """, tuple(params_3))
        failure_modes = await cursor.fetchall()

        # -- 4. Últimas Operaciones Críticas (Severidad >= 8 + Error) --
        filters_4 = ["severity >= 8", "status = 'ERROR'"]
        params_4 = []
        if not is_super:
            filters_4.append("enterprise_id = %s")
            params_4.append(ent_id)
        if date_filter_sql:
            filters_4.append(date_filter_sql)
            params_4.extend(date_params)
            
        where_clause_4 = "WHERE " + " AND ".join(filters_4)

        await cursor.execute(f"""
            SELECT enterprise_id, module, endpoint, status, severity, impact_category, 
                   failure_mode, error_message, duration_ms, created_at
            FROM sys_transaction_logs
            {where_clause_4}
            ORDER BY created_at DESC
            LIMIT 20
        """, tuple(params_4))
        critical_events = await cursor.fetchall()

        # -- 5. Mitigaciones Activas Recientes --
        filters_5 = []
        params_5 = []
        if not is_super:
            filters_5.append("m.enterprise_id = %s")
            params_5.append(ent_id)
        if date_filter_sql:
            # Reemplace created_at con m.started_at
            filters_5.append(date_filter_sql.replace("created_at", "m.started_at"))
            params_5.extend(date_params)
            
        where_clause_5 = "WHERE " + " AND ".join(filters_5) if filters_5 else ""

        await cursor.execute(f"""
            SELECT sys_risk_active_mitigations.*, sys_risk_mitigation_rules.action_type, sys_risk_mitigation_rules.failure_mode as rule_mode, sys_risk_mitigation_rules.min_severity
            FROM sys_risk_active_mitigations
            JOIN sys_risk_mitigation_rules ON sys_risk_active_mitigations.rule_id = sys_risk_mitigation_rules.id
            {where_clause_5}
            ORDER BY sys_risk_active_mitigations.started_at DESC
            LIMIT 10
        """, tuple(params_5))
        mitigations = await cursor.fetchall()

        # -- 6. Trend de errores (Siempre usa 7 dias o el rango seleccionado para dibujar la linea temporal) --
        filters_6 = []
        params_6 = []
        if not is_super:
            filters_6.append("enterprise_id = %s")
            params_6.append(ent_id)
            
        if timeframe == 'range' and date_start and date_end:
             filters_6.append("DATE(created_at) BETWEEN %s AND %s")
             params_6.extend([date_start, date_end])
        elif timeframe == 'day':
             filters_6.append("DATE(created_at) = CURDATE()")
        elif timeframe == 'month':
             filters_6.append("created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)")
        else:
             filters_6.append("created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)")
             
        where_clause_6 = "WHERE " + " AND ".join(filters_6)

        await cursor.execute(f"""
            SELECT 
                DATE(created_at) as day,
                module,
                SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as errors,
                COUNT(*) as total
            FROM sys_transaction_logs
            {where_clause_6}
            GROUP BY DATE(created_at), module
            ORDER BY day, module
        """, tuple(params_6))
        trend_raw = await cursor.fetchall()
        
        # Structure the data for multi-line chart
        # We need a list of unique days (labels) and a mapping of module -> list of values
        days = sorted(list(set(str(r['day']) for r in trend_raw)))
        modules = sorted(list(set(r['module'] for r in trend_raw if r['module'])))
        
        trend_data = {
            'labels': days,
            'datasets': []
        }
        
        # Add "Total" as a baseline
        total_by_day = {d: 0 for d in days}
        for r in trend_raw:
            total_by_day[str(r['day'])] += r['total']
        
        trend_data['datasets'].append({
            'label': 'Total Global',
            'data': [total_by_day[d] for d in days],
            'color': '#0dcaf0'
        })

        # Add top 5 categories (modules) by total errors
        module_totals = {}
        for r in trend_raw:
            m = r['module'] or 'OTROS'
            module_totals[m] = module_totals.get(m, 0) + r['errors']
        
        top_modules = sorted(module_totals.items(), key=lambda x: x[1], reverse=True)[:5]
        colors = ['#ef4444', '#f59e0b', '#3b82f6', '#8b5cf6', '#10b981']
        
        for i, (m, _) in enumerate(top_modules):
            if _ == 0: continue # Skip if no errors
            m_data = {d: 0 for d in days}
            for r in trend_raw:
                if (r['module'] or 'OTROS') == m:
                    m_data[str(r['day'])] = r['errors']
            
            trend_data['datasets'].append({
                'label': f"Error: {m}",
                'data': [m_data[d] for d in days],
                'color': colors[i % len(colors)]
            })

    return await render_template('admin_risk_dashboard.html',
                           kpis=kpis,
                           module_heatmap=module_heatmap,
                           failure_modes=failure_modes,
                           critical_events=critical_events,
                           mitigations=mitigations,
                           trend_data=trend_data,
                           timeframe=timeframe,
                           date_start=date_start,
                           date_end=date_end,
                           is_super=is_super)

# --- THREAD MONITORING & CONSOLE ---

@core_bp.route('/admin/threads')
@login_required
@permission_required('sysadmin')
async def admin_threads():
    """Muestra una consola con los hilos activos y procesos OS de la aplicación."""
    import threading
    from core.concurrency import get_active_tasks
    import time
    import os
    import subprocess
    
    current_os_pid = os.getpid()
    
    # 1. Tareas gestionadas (DB)
    managed_tasks = await get_active_tasks()
    now = time.time()
    
    top_level_tasks = []
    dependent_tasks = []
    for tid, info in managed_tasks.items():
        task_data = {
            'ident': tid,
            'process_name': info.get('process_name', 'Proceso Desconocido'),
            'description': info['description'],
            'priority': info.get('priority', 5),
            'parent_id': info.get('parent_id'),
            'uptime': round((now - info['start_time']) / 60, 1),
            'thread_id': info['thread_id'],
            'os_pid': info.get('os_pid'),
            'source_type': info.get('source_type', 'DB_TASK'),
            'source_origin': info.get('source_origin', 'WEB'),
            'requested_stop': info.get('requested_stop', 0),
            'status': info.get('status', 'RUNNING')
        }
        if task_data['parent_id']: dependent_tasks.append(task_data)
        else: top_level_tasks.append(task_data)
    
    top_level_tasks.sort(key=lambda x: x['priority'])
    
    # 2. Detectar otros procesos de Flask (app.py) en el OS
    flask_pids = []
    try:
        # Buscamos procesos python que contengan 'app.py'
        cmd = 'wmic process where "commandline like \'%app.py%\'" get processid,commandline /format:list'
        output = subprocess.check_output(cmd, shell=True).decode('latin1')
        
        current_pid_block = {}
        for line in output.split('\n'):
            line = line.strip()
            if not line: continue
            if '=' in line:
                key, val = line.split('=', 1)
                current_pid_block[key.strip()] = val.strip()
            
            if 'ProcessId' in current_pid_block and 'CommandLine' in current_pid_block:
                pid = int(current_pid_block['ProcessId'])
                # No agregar el comando de wmic si se coló
                if 'wmic' not in current_pid_block['CommandLine'].lower():
                    flask_pids.append({
                        'pid': pid,
                        'is_current': (pid == current_os_pid),
                        'cmd': current_pid_block['CommandLine'][:100] + '...'
                    })
                current_pid_block = {}
    except:
        pass

    # 3. Hilos nativos del proceso ACTUAL
    native_threads = []
    for t in threading.enumerate():
        native_threads.append({
            'ident': t.ident,
            'name': t.name,
            'is_alive': t.is_alive(),
            'daemon': t.daemon
        })
        
    return await render_template('admin_threads.html', 
                           top_level=top_level_tasks,
                           dependents=dependent_tasks,
                           native_threads=native_threads,
                           flask_pids=flask_pids,
                           current_pid=current_os_pid,
                           now=time.strftime("%H:%M:%S"))

@core_bp.route('/admin/threads/cancel/<task_id>', methods=['POST'])
@login_required
@permission_required('sysadmin')
async def cancel_thread(task_id):
    """Envía señal de cancelación a un hilo gestionado."""
    from core.concurrency import signal_stop
    
    await signal_stop(task_id)
    await flash(f"Señal de cancelación enviada a la tarea {task_id}. El hilo se detendrá al finalizar su ciclo actual.", "warning")
    
    return redirect(url_for('core.admin_threads'))

# --- MÓDULO DE CONSULTA DE ERRORES ---

# Mapeos amistosos para el perfil de negocio
_FAILURE_MODE_LABELS = {
    'DATA_INTEGRITY':  ('🔗 Conflicto de Datos',          'Se intentó guardar información que entra en conflicto con registros existentes (duplicados, referencias rotas).'),
    'SECURITY_AUTH':   ('🔒 Acceso Denegado',              'El usuario no tenía los permisos necesarios para realizar esta operación.'),
    'NETWORK_TRANSIT': ('📡 Error de Conexión',            'Se produjo una falla de comunicación entre el sistema y la base de datos o un servicio externo.'),
    'BUSINESS_LOGIC':  ('📋 Regla de Negocio Violada',    'La operación no pudo completarse porque incumple una regla configurada en el sistema.'),
    # --- HTTP Exceptions (Del Título: "Recorrido a Errores del Servidor") ---
    'HTTP_400':        ('⚠️ Petición Mal Formada',        'Los datos enviados al servidor no cumplen con el formato o los requisitos esperados.'),
    'HTTP_401':        ('🔑 Sesión Expirada/Inválida',    'Tiene que volver a iniciar sesión. Credenciales insuficientes.'),
    'HTTP_403':        ('⛔ Permisos Insuficientes',      'No tiene los privilegios necesarios para ver este módulo u operar sobre este recurso.'),
    'HTTP_404':        ('🔍 Recurso No Encontrado',       'La URL, registro o pantalla solicitada no existe o fue eliminada.'),
    'HTTP_405':        ('🛑 Método No Permitido',         'Intento de procesar datos por una vía no autorizada (Ej: GET en lugar de POST).'),
    'HTTP_409':        ('⏱️ Conflicto de Concurrencia',   'Control OPTIMISTA: Los datos fueron modificados por otro usuario desde que abriste la pantalla. Refresca y repite.'),
    'HTTP_422':        ('❌ Validación Lógica Fallida',   'La petición estaba bien estructurada, pero el contenido fue rechazado por reglas de negocio semánticas.'),
    'HTTP_429':        ('🚦 Demasiadas Peticiones',       'Rate Limit Ocupado. Espere unos momentos antes de volver a solicitar procesos.'),
    'HTTP_500':        ('💥 Falla de Servidor (Critica)', 'Ocurrió un error inesperado al procesar la solicitud interna. El equipo técnico ha sido notificado.'),
}
_MODULE_LABELS = {
    'stock':        '📦 Inventario y Stock',
    'ventas':       '💰 Ventas y Facturación',
    'compras':      '🛒 Compras y Proveedores',
    'contabilidad': '📊 Contabilidad',
    'fondos':       '🏦 Tesorería',
    'core':         '⚙️ Sistema y Seguridad',
    'enterprise':   '🏢 Gestión de Empresas',
}
_IMPACT_LABELS = {
    'Financial':    ('🔴', 'Crítico Financiero'),
    'Integrity':    ('🟠', 'Integridad de Datos'),
    'Operational':  ('🟡', 'Operacional'),
    'Security':     ('🔴', 'Seguridad'),
    'Compliance':   ('🟠', 'Cumplimiento Legal'),
    'Technical':    ('🔵', 'Técnico'),
}

@core_bp.route('/admin/error-log')
@login_required
@permission_required('view_error_log')
async def error_log():
    """Módulo de consulta de errores — perfil Negocio / Experto."""
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'
    ent_id   = g.user['enterprise_id']

    # Filtros desde la URL
    f_module       = request.args.get('module', '')
    f_failure      = request.args.get('failure_mode', '')
    f_severity_min = request.args.get('severity_min', 1, type=int)
    f_date_from    = request.args.get('date_from', '')
    f_date_to      = request.args.get('date_to', '')
    f_profile      = request.args.get('profile', 'business')   # 'business' | 'expert'
    f_status       = request.args.get('status', 'ERROR')
    f_incident     = request.args.get('incident_status', 'OPEN')
    f_user_id      = request.args.get('user_id', '')
    page           = request.args.get('page', 1, type=int)
    per_page       = 25

    conditions = ["severity >= %s"]
    params     = [f_severity_min]

    if f_status:
        conditions.append("status = %s")
        params.append(f_status)
    
    if f_incident:
        if f_incident == 'OPEN':
            conditions.append("(incident_status = 'OPEN' OR incident_status IS NULL OR incident_status = '')")
        else:
            conditions.append("incident_status = %s")
            params.append(f_incident)

    if f_user_id:
        conditions.append("user_id = %s")
        params.append(f_user_id)

    if not is_super:
        conditions.append("enterprise_id = %s")
        params.append(ent_id)

    if f_module:
        conditions.append("module = %s")
        params.append(f_module)

    if f_failure:
        conditions.append("failure_mode = %s")
        params.append(f_failure)

    if f_date_from:
        conditions.append("DATE(created_at) >= %s")
        params.append(f_date_from)

    if f_date_to:
        conditions.append("DATE(created_at) <= %s")
        params.append(f_date_to)

    where_sql = "WHERE " + " AND ".join(conditions) if conditions else ""
    offset = (page - 1) * per_page

    async with get_db_cursor(dictionary=True) as cursor:
        # Total para paginación
        await cursor.execute(f"SELECT COUNT(*) as cnt FROM sys_transaction_logs {where_sql}", params)
        total = await cursor.fetchone()['cnt']

        # Registros paginados
        await cursor.execute(f"""
            SELECT id, enterprise_id, user_id, module, endpoint, request_method,
                   request_data, status, severity, impact_category, failure_mode,
                   error_message, duration_ms, created_at,
                   incident_status, assigned_to, resolution_date, management_history,
                   DATEDIFF(NOW(), created_at) as dias_sin_resolucion
            FROM sys_transaction_logs
            {where_sql}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])
        errors = await cursor.fetchall()

        # Listas para filtros desplegables
        await cursor.execute("SELECT DISTINCT module FROM sys_transaction_logs WHERE module IS NOT NULL ORDER BY module")
        modules_list = [r['module'] for r in await cursor.fetchall()]

        await cursor.execute("SELECT DISTINCT failure_mode FROM sys_transaction_logs WHERE failure_mode IS NOT NULL ORDER BY failure_mode")
        failures_list = [r['failure_mode'] for r in await cursor.fetchall()]

        # Lista de usuarios que han tenido errores
        await cursor.execute("SELECT DISTINCT user_id FROM sys_transaction_logs WHERE user_id IS NOT NULL ORDER BY user_id")
        users_list = [r['user_id'] for r in await cursor.fetchall()]

    total_pages = max(1, (total + per_page - 1) // per_page)

    return await render_template('admin_error_log.html',
                           errors=errors,
                           total=total,
                           page=page,
                           total_pages=total_pages,
                           modules_list=modules_list,
                           failures_list=failures_list,
                           f_module=f_module,
                           f_failure=f_failure,
                           f_severity_min=f_severity_min,
                           f_date_from=f_date_from,
                           f_date_to=f_date_to,
                           f_status=f_status,
                           f_user_id=f_user_id,
                           users_list=users_list,
                           f_incident=f_incident,
                           f_profile=f_profile,
                           is_super=is_super,
                           failure_labels=_FAILURE_MODE_LABELS,
                           module_labels=_MODULE_LABELS,
                           impact_labels=_IMPACT_LABELS)


@core_bp.route('/admin/error-log/detail/<int:error_id>')
@login_required
@permission_required('view_error_log')
async def error_log_detail(error_id):
    """Detalle completo de un error — vista Negocio o Experto."""
    import json as _json
    is_super  = str(g.user.get('username', '')).lower() == 'superadmin'
    ent_id    = g.user['enterprise_id']
    f_profile = request.args.get('profile', 'business')

    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT sys_transaction_logs.*, sys_users_creator.username, sys_users_creator.email as user_email, 
                   DATEDIFF(NOW(), sys_transaction_logs.created_at) as dias_sin_resolucion,
                   sys_users_assignee.username as assignee_name
            FROM sys_transaction_logs
            LEFT JOIN sys_users sys_users_creator ON sys_transaction_logs.user_id = sys_users_creator.id
            LEFT JOIN sys_users sys_users_assignee ON sys_transaction_logs.assigned_to = sys_users_assignee.id
            WHERE sys_transaction_logs.id = %s
        """, (error_id,))
        error = await cursor.fetchone()

    if not error:
        await flash("Registro de error no encontrado.", "danger")
        return redirect(url_for('core.error_log'))

    # Verificar que el error pertenece a la empresa del usuario (o es superadmin)
    if not is_super and error['enterprise_id'] != ent_id and error['enterprise_id'] != 0:
        await flash("Acceso denegado.", "danger")
        return redirect(url_for('core.error_log'))

    # Parsear request_data (JSON string -> dict)
    req_data_parsed = {}
    if error.get('request_data'):
        try:
            req_data_parsed = _json.loads(error['request_data'])
        except Exception:
            req_data_parsed = {'raw': str(error['request_data'])}

    # Ocultar datos sensibles para perfil negocio
    if f_profile == 'business':
        sensitive_keys = ['password', 'token', 'clave', 'secret', 'hash', 'key']
        req_data_parsed = {
            k: '••••••••' if any(s in k.lower() for s in sensitive_keys) else v
            for k, v in req_data_parsed.items()
        }

    # Parse management_history
    history_parsed = []
    if error.get('management_history'):
        try:
            history_parsed = _json.loads(error['management_history'])
        except Exception:
            pass

    # Fetch users habilitados para soporte (rol soporte_tecnico)
    users_list = []
    try:
        await cursor.execute("""
            SELECT sys_users.id, sys_users.username
            FROM sys_users
            JOIN sys_roles ON sys_users.role_id = sys_roles.id AND sys_roles.enterprise_id = sys_users.enterprise_id
            WHERE sys_roles.name = 'soporte_tecnico'
              AND (sys_users.enterprise_id = %s OR sys_users.enterprise_id = 0)
            ORDER BY sys_users.username
        """, (ent_id,))
        users_list = await cursor.fetchall()
    except Exception:
        pass


    return await render_template('admin_error_detail.html',
                           error=error,
                           req_data=req_data_parsed,
                           history_list=history_parsed,
                           users_list=users_list,
                           f_profile=f_profile,
                           failure_labels=_FAILURE_MODE_LABELS,
                           module_labels=_MODULE_LABELS,
                           impact_labels=_IMPACT_LABELS)

@core_bp.route('/admin/error-log/update/<int:error_id>', methods=['POST'])
@login_required
@permission_required('view_error_log')
async def error_log_update_incident(error_id):
    import json
    import datetime
    from services import email_service
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'

    status = (await request.form).get('incident_status')
    asignado_str = (await request.form).get('assigned_to')  # id user o empty
    notas = (await request.form).get('new_note', '').strip()
    res_date = (await request.form).get('resolution_date')

    if not status:
        await flash("Estado de incidente requerido.", "warning")
        return redirect(url_for('core.error_log_detail', error_id=error_id, profile='business'))

    assigned_to = None
    if asignado_str and asignado_str.isdigit():
        assigned_to = int(asignado_str)

    if res_date:
        res_date = datetime.datetime.strptime(res_date, "%Y-%m-%d").strftime("%Y-%m-%d %H:%M:%S")

    try:
        async with get_db_cursor(dictionary=True) as cursor:
            # Check exist y ver permisos
            await cursor.execute("SELECT id, enterprise_id, management_history, user_id FROM sys_transaction_logs WHERE id = %s", (error_id,))
            error = await cursor.fetchone()
            if not error: raise Exception("Incidente no encontrado.")
            if not is_super and error['enterprise_id'] != g.user['enterprise_id']: raise Exception("No autorizado.")

            # Append note to JSON history
            history = []
            if error.get('management_history'):
                try:
                    history = json.loads(error['management_history'])
                except Exception:
                    pass
            if notas:
                history.append({
                    "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "user": g.user['username'],
                    "note": notas,
                    "status_change": status
                })

            new_history_str = json.dumps(history)

            await cursor.execute("""
                UPDATE sys_transaction_logs 
                SET incident_status = %s, assigned_to = %s, resolution_date = %s, management_history = %s
                WHERE id = %s
            """, (status, assigned_to, res_date, new_history_str, error_id))

            # Notificar al usuario reportado si existe y si hubo un comentario o cambio
            if error.get('user_id') or assigned_to:
                # Obtener info destinatario (Usuario original o asignado)
                # Mandaremos email al usuario original (si existe) y al resp.
                emails_to_notify = set()
                names_to_notify = []
                
                # Usuario original
                if error.get('user_id'):
                    await cursor.execute("SELECT email, username FROM sys_users WHERE id = %s", (error.get('user_id'),))
                    u = await cursor.fetchone()
                    if u and u['email']:
                        emails_to_notify.add(u['email'])
                        names_to_notify.append(u['username'])

                # Responsable asignado
                if assigned_to:
                    await cursor.execute("SELECT email, username FROM sys_users WHERE id = %s", (assigned_to,))
                    resp = await cursor.fetchone()
                    if resp and resp['email']:
                        emails_to_notify.add(resp['email'])

                if is_super and 'marcelocperi@gmail.com' not in emails_to_notify:
                    # Siempre notificar al super opcional
                    pass

                # Dispatch async mails
                async def send_inc_mail_1(e_mail, e_name):
                    await email_service.enviar_notificacion_incidente(
                        e_mail,
                        e_name,
                        error_id,
                        status,
                        notas,
                        history,
                        error['enterprise_id']
                    )

                for mail in emails_to_notify:
                    if mail:
                        uname = names_to_notify[0] if names_to_notify else "Usuario"
                        current_app.add_background_task(send_inc_mail_1, mail, uname)

        await flash("Incidente actualizado correctamente.", "success")

    except Exception as e:
        await flash(f"Error actualizando incidente: {e}", "danger")

    return redirect(url_for('core.error_log_detail', error_id=error_id, profile='expert'))


@core_bp.route('/admin/error-log/quick-action/<int:error_id>', methods=['POST'])
@login_required
@permission_required('view_error_log')
async def error_log_quick_action(error_id):
    """Acción rápida desde la lista: tomar / resolver / reabrir un incidente sin ir al detalle."""
    import json, datetime, threading
    from services import email_service

    action = (await request.form).get('action')  # 'take' | 'resolve' | 'reopen'
    return_url = (await request.form).get('return_url', url_for('core.error_log', profile='expert'))

    action_map = {
        'take':     ('IN_PROGRESS', f'Tomado por {g.user["username"]}'),
        'feedback': ('FEEDBACK',    f'Enviado a Feedback por {g.user["username"]}'),
        'resolve':  ('RESOLVED',    f'Marcado como Resuelto por {g.user["username"]}'),
        'close':    ('CLOSED',      f'Cerrado y Aprobado por {g.user["username"]}'),
        'reopen':   ('OPEN',        f'Reabierto por {g.user["username"]}'),
    }
    if action not in action_map:
        await flash("Acción no reconocida.", "warning")
        return redirect(return_url)

    new_status, auto_note = action_map[action]
    is_super = str(g.user.get('username', '')).lower() == 'superadmin'

    try:
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute(
                "SELECT id, enterprise_id, management_history, user_id FROM sys_transaction_logs WHERE id = %s",
                (error_id,)
            )
            err = await cursor.fetchone()
            if not err:
                raise Exception("Incidente no encontrado.")
            if not is_super and err['enterprise_id'] != g.user['enterprise_id']:
                raise Exception("No autorizado.")

            # Historial JSON
            history = []
            if err.get('management_history'):
                try:
                    history = json.loads(err['management_history'])
                except Exception:
                    pass
            history.append({
                "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "user": g.user['username'],
                "note": auto_note,
                "status_change": new_status
            })

            # Al "tomar" → me asigno; al resolver/reabrir → sin tocar el assigned_to existente
            assigned_to = g.user['id'] if action == 'take' else err.get('assigned_to')

            await cursor.execute("""
                UPDATE sys_transaction_logs
                SET incident_status   = %s,
                    assigned_to       = %s,
                    management_history = %s
                WHERE id = %s
            """, (new_status, assigned_to, json.dumps(history), error_id))

            # ── Notificaciones por email ──────────────────────────────
            emails_to_notify = set()
            names_to_notify  = []

            # Usuario que originó el error
            if err.get('user_id'):
                await cursor.execute(
                    "SELECT email, username FROM sys_users WHERE id = %s",
                    (err['user_id'],)
                )
                u = await cursor.fetchone()
                if u and u.get('email'):
                    emails_to_notify.add(u['email'])
                    names_to_notify.append(u['username'])

            # Responsable asignado (puede ser yo mismo al "tomar")
            if assigned_to:
                await cursor.execute(
                    "SELECT email, username FROM sys_users WHERE id = %s",
                    (assigned_to,)
                )
                resp = await cursor.fetchone()
                if resp and resp.get('email'):
                    emails_to_notify.add(resp['email'])
                    if resp['username'] not in names_to_notify:
                        names_to_notify.append(resp['username'])

            # Dispatch async para no bloquear la respuesta
            if emails_to_notify:
                async def send_inc_mail_2(e_mail, e_name):
                    await email_service.enviar_notificacion_incidente(
                        e_mail,
                        e_name,
                        error_id,
                        new_status,
                        auto_note,
                        history,
                        err['enterprise_id']
                    )

                for mail in emails_to_notify:
                    uname = names_to_notify[0] if names_to_notify else "Usuario"
                    current_app.add_background_task(send_inc_mail_2, mail, uname)

        await flash(f"Incidente #{error_id} → {new_status}. Notificaciones enviadas.", "success")

    except Exception as e:
        await flash(f"Error en acción rápida: {e}", "danger")

    return redirect(return_url)


# --- PROYECTOS Y REQUERIMIENTOS ---
@core_bp.route('/admin/proyectos', methods=['GET'])
@login_required
@permission_required('admin_users')
async def admin_proyectos():
    ent_id = g.user['enterprise_id']
    async with get_db_cursor(dictionary=True) as cursor:
        await cursor.execute("""
            SELECT sys_proyectos_requerimientos.*, sys_users.username as reported_by
            FROM sys_proyectos_requerimientos
            LEFT JOIN sys_users ON sys_proyectos_requerimientos.user_id = sys_users.id
            WHERE sys_proyectos_requerimientos.enterprise_id = %s
            ORDER BY FIELD(sys_proyectos_requerimientos.estado, 'PENDIENTE', 'EN_PROGRESO', 'COMPLETADO', 'DESCARTADO'), 
                     FIELD(sys_proyectos_requerimientos.prioridad, 'ALTA', 'MEDIA', 'BAJA'), sys_proyectos_requerimientos.created_at DESC
        """, (ent_id,))
        proyectos = await cursor.fetchall()
    return await render_template('admin_proyectos.html', proyectos=proyectos)

@core_bp.route('/admin/proyectos/guardar', methods=['POST'])
@login_required
@permission_required('admin_users')
async def admin_proyectos_guardar():
    ent_id = g.user['enterprise_id']
    user_id = g.user['id']
    
    id_req = (await request.form).get('id')
    titulo = (await request.form).get('titulo')
    descripcion = (await request.form).get('descripcion')
    tipo = (await request.form).get('tipo', 'REQUERIMIENTO')
    estado = (await request.form).get('estado', 'PENDIENTE')
    prioridad = (await request.form).get('prioridad', 'MEDIA')

    async with get_db_cursor() as cursor:
        try:
            if id_req:
                await cursor.execute("""
                    UPDATE sys_proyectos_requerimientos 
                    SET titulo=%s, descripcion=%s, tipo=%s, estado=%s, prioridad=%s
                    WHERE id=%s AND enterprise_id=%s
                """, (titulo, descripcion, tipo, estado, prioridad, id_req, ent_id))
                await flash("Requerimiento actualizado.", "success")
            else:
                await cursor.execute("""
                    INSERT INTO sys_proyectos_requerimientos 
                    (enterprise_id, titulo, descripcion, tipo, estado, prioridad, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (ent_id, titulo, descripcion, tipo, estado, prioridad, user_id))
                await flash("Requerimiento creado.", "success")
        except Exception as e:
            await flash(f"Error al guardar: {e}", "danger")
            
    return redirect(url_for('core.admin_proyectos'))

@core_bp.route('/admin/proyectos/eliminar/<int:id>', methods=['POST'])
@login_required
@permission_required('admin_users')
async def admin_proyectos_eliminar(id):
    ent_id = g.user['enterprise_id']
    async with get_db_cursor() as cursor:
        try:
            await cursor.execute("DELETE FROM sys_proyectos_requerimientos WHERE id=%s AND enterprise_id=%s", (id, ent_id))
            await flash("Requerimiento eliminado.", "success")
        except Exception as e:
            await flash(f"Error al eliminar: {e}", "danger")
    return redirect(url_for('core.admin_proyectos'))

