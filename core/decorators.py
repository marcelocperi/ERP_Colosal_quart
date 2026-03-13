
from functools import wraps
from quart import g, request, redirect, url_for, flash, jsonify

def _is_ajax_or_fetch():
    """Detecta si la petición viene de fetch() o AJAX y espera JSON, no HTML."""
    # 1. Header explícito de XMLHttpRequest
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return True
    # 2. Accept header indica que quiere JSON
    accept = request.headers.get('Accept', '')
    if 'application/json' in accept:
        return True
    # 3. Content-Type es JSON (POST con body JSON)
    content_type = request.headers.get('Content-Type', '')
    if 'application/json' in content_type:
        return True
    # 4. Heurística: fetch() por defecto envía Accept: */*
    #    Los navegadores normales envían text/html como preferido
    if 'text/html' not in accept and accept != '':
        return True
    return False

async def _unauthorized_response(message="Sesión expirada o inválida. Recargue la página."):
    """Respuesta apropiada para cuando el usuario no está autenticado, y loggea el incidente."""
    # Intentar loguear el 401 Unauthorized
    try:
        from database import get_db_cursor
        import json
        req_data = {}
        try:
            if request.is_json: req_data = await request.json
            elif (await request.form): req_data = dict(await request.form)
        except: pass
        clob = {
            'request_path': request.path,
            'referrer': request.referrer,
            'reason': message
        }
        ent_id = (await request.form).get('enterprise_id') or request.args.get('enterprise_id') or 0
        try: ent_id = int(ent_id)
        except: ent_id = 0
            
        from quart import session, g
        sid = getattr(g, 'sid', None) or session.get('session_id')
            
        async with get_db_cursor() as log_cursor:
            await log_cursor.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'clob_data'")
            has_clob = bool(await log_cursor.fetchone())
            col = 'clob_data' if has_clob else 'error_traceback'
            await log_cursor.execute(f"""
                INSERT INTO sys_transaction_logs 
                (enterprise_id, user_id, session_id, module, endpoint, request_method, request_data, 
                 status, severity, impact_category, failure_mode, error_message, {col})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (ent_id, None, sid, 'AUTH', request.path, request.method, json.dumps(req_data),
                  'ERROR', 5, 'SECURITY', 'HTTP_401', message, json.dumps(clob)))
    except Exception:
        pass # Fallback silencioso si falla el log

    if _is_ajax_or_fetch():
        return jsonify({"error": message, "redirect": url_for('core.login')}), 401
    return redirect(url_for('core.login'))
import inspect

def login_required(view):
    @wraps(view)
    async def wrapped_view(**kwargs):
        if g.user is None:
            return await _unauthorized_response()
        if inspect.iscoroutinefunction(view):
            return await view(**kwargs)
        return view(**kwargs)
    return wrapped_view

async def _log_forbidden_try(permission_code, user_msg):
    try:
        from database import get_db_cursor
        import json, traceback
        
        req_data = {}
        try:
            if request.is_json: req_data = await request.json
            elif (await request.form): req_data = dict(await request.form)
        except: pass
        
        clob = {
            'request_path': request.path,
            'referrer': request.referrer,
            'missing_permission': permission_code,
            'reason': user_msg
        }
        
        user_id = getattr(g, 'user', {}).get('id') if getattr(g, 'user', None) else None
        ent_id = getattr(g, 'user', {}).get('enterprise_id', 0) if getattr(g, 'user', None) else 0

        from quart import session, g
        sid = getattr(g, 'sid', None) or session.get('session_id')

        async with get_db_cursor() as log_cursor:
            await log_cursor.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'clob_data'")
            has_clob = bool(await log_cursor.fetchone())
            col = 'clob_data' if has_clob else 'error_traceback'
            await log_cursor.execute(f"""
                INSERT INTO sys_transaction_logs 
                (enterprise_id, user_id, session_id, module, endpoint, request_method, request_data, 
                 status, severity, impact_category, failure_mode, error_message, {col})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (ent_id, user_id, sid, 'SECURITY', request.path, request.method, json.dumps(req_data),
                  'ERROR', 6, 'SECURITY', 'HTTP_403', user_msg, json.dumps(clob)))
    except Exception:
        pass

def permission_required(permission_code):
    def decorator(view):
        @wraps(view)
        async def wrapped_view(**kwargs):
            if g.user is None:
                return await _unauthorized_response()
            
            # superadmin bypass
            if str(g.user.get('username', '')).lower() == 'superadmin':
                if inspect.iscoroutinefunction(view):
                    return await view(**kwargs)
                return view(**kwargs)

            if permission_code == 'sysadmin':
                if 'sysadmin' not in g.permissions:
                     msg = "Acceso Denegado. Se requiere nivel Super Administrador."
                     await _log_forbidden_try('sysadmin', msg)
                     if _is_ajax_or_fetch():
                         return jsonify({"error": msg}), 403
                     await flash(f"Acceso Denegado: Se requiere nivel Super Administrador.", "danger")
                     return redirect(url_for('ventas.dashboard'))
                if inspect.iscoroutinefunction(view):
                    return await view(**kwargs)
                return view(**kwargs)

            if 'all' in g.permissions:
                if inspect.iscoroutinefunction(view):
                    return await view(**kwargs)
                return view(**kwargs)
            
            if permission_code not in g.permissions:
                msg = f"Acceso Denegado: Se requiere permiso '{permission_code}'"
                await _log_forbidden_try(permission_code, msg)
                if _is_ajax_or_fetch():
                    return jsonify({"error": msg}), 403
                await flash(msg, "danger")
                if request.endpoint == 'ventas.dashboard' or request.endpoint == 'dashboard':
                    return "Acceso insuficiente. Contacte al administrador.", 403
                return redirect(url_for('ventas.dashboard'))
            
            if inspect.iscoroutinefunction(view):
                return await view(**kwargs)
            return view(**kwargs)
        return wrapped_view
    return decorator
