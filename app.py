
from quart import Quart, session, g, request, url_for
from core.routes import core_bp
from core.enterprise_admin import ent_bp
from ventas.routes import ventas_bp
from compras.routes import compras_bp
from stock.routes import stock_bp
from contabilidad.routes import contabilidad_bp
from fondos.routes import fondos_bp
from utilitarios.routes import utilitarios_bp
from core.routes import _log_security_event
from pricing.routes import pricing_bp
from produccion.routes import produccion_bp
from database import DB_CONFIG, get_db_cursor
import os
import secrets
import datetime
import logging
import time

import decimal
from quart.json.provider import DefaultJSONProvider

class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return super().default(obj)

app = Quart(__name__)
app.json = CustomJSONProvider(app)
class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'bibliotecaweb-secret-key-multi-mcp-2024')
    PERMANENT_SESSION_LIFETIME = datetime.timedelta(hours=8)
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    # En redes locales (192.168.x.x) HTTP estándar, Android/Chrome bloquea cookies si SECURE=True
    SESSION_COOKIE_SECURE = os.environ.get('SECURE_COOKIES', 'False').lower() in ('true', '1', 'yes')

app.config.from_object(Config)

# Configuración de Logging Dual (Consola + Archivo)
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app_startup.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Register Blueprints
app.register_blueprint(core_bp)
app.register_blueprint(ent_bp)
app.register_blueprint(ventas_bp)
app.register_blueprint(compras_bp)
app.register_blueprint(stock_bp)
app.register_blueprint(contabilidad_bp)
app.register_blueprint(fondos_bp)
app.register_blueprint(utilitarios_bp)
app.register_blueprint(pricing_bp)
app.register_blueprint(produccion_bp)

@app.before_serving
async def initialize_all_services():
    """Inicialización centralizada de servicios en el arranque de Quart."""
    try:
        from database import get_db_pool, get_db_cursor
        from services.georef_service import GeorefService
        from services.erp_master_service import ErpMasterService
        
        # 1. Pool de DB
        pool = await get_db_pool()
        if pool:
            async with get_db_cursor() as cur:
                await cur.execute("SELECT 1")
            logger.info("✅ DB Pool asíncrono pre-inicializado.")
        
        # 2. Servicios de negocio
        await GeorefService.initialize_db()
        await ErpMasterService.initialize_db()
        logger.info("✅ Servicios Georef/ERP inicializados.")
        
    except Exception as e:
        logger.error(f"❌ Error crítico en initialize_services: {e}")

@app.after_request
async def add_header(response):
    """
    Add headers to both force latest IE rendering engine or chrome frame,
    and also to cache the rendered page for 0 seconds.
    """
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response


# from services.cm05_routes import cm05_api_bp
# app.register_blueprint(cm05_api_bp)

# from services.ai_chat_routes import ai_chat_bp
# app.register_blueprint(ai_chat_bp)

# El manejo de permisos y caché ahora se centraliza en services.session_service.SessionDispatcher


@app.before_request
async def security_and_auth():
    # Initialize globals first to avoid AttributeError in logs
    g.user = None
    g.permissions = []
    
    # 1. CSRF Protection
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(16)
        session.modified = True
    
    if request.method == 'POST':
        # Skip CSRF for login and static/API routes that might not support it yet
        exempt_paths = [url_for('core.login'), '/api/clima', '/api/finance']
        if request.path not in exempt_paths:
            token = (await request.form).get('csrf_token') or request.headers.get('X-CSRF-Token')
            session_token = session.get('csrf_token')
            
            if not token or token != session_token:
                logger.warning(f"CSRF Failure on {request.path}. Form Token: {token[:5] if token else 'None'}..., Session Token: {session_token[:5] if session_token else 'None'}...")
                _log_security_event("CSRF_ATTEMPT", "FAILURE", details=f"Invalid or missing token on {request.path}")
                # Devolver JSON para peticiones AJAX/fetch
                accept = request.headers.get('Accept', '')
                content_type = request.headers.get('Content-Type', '')
                if 'application/json' in accept or 'application/json' in content_type or 'text/html' not in accept:
                    from quart import jsonify
                    return jsonify({"error": "Token CSRF inválido. Recargue la página."}), 403
                return "Error de Seguridad: Token CSRF inválido. Intente recargar la página.", 403

    # 2. Despachador de Sesión Independiente (Dispatcher)
    # Procesa la identidad del usuario, empresa y permisos de forma centralizada.
    from services.session_service import SessionDispatcher
    await SessionDispatcher.attach_session_context()
    
    # 3. Registro de incidentes si se intenta acceder a rutas protegidas sin sesión válida
    # (La validación final ocurre en los decoradores @login_required)



@app.after_request
async def add_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    # response.headers['Content-Security-Policy'] = "default-src 'self' https: 'unsafe-inline' 'unsafe-eval'; img-src 'self' data: https:;"
    return response

@app.url_defaults
def add_sid_to_url(endpoint, values):
    """
    Inyecta automáticamente el 'sid' en todas las llamadas a url_for.
    Esto evita tener que pasar sid=sid manualmente en cada link del template.
    """
    if 'sid' not in values and hasattr(g, 'sid') and g.sid:
        values.setdefault('sid', g.sid)

@app.url_value_preprocessor
def pull_sid_from_url(endpoint, values):
    """
    Extrae el 'sid' de los valores de la URL antes de que lleguen a la vista.
    """
    if values and 'sid' in values:
        g.sid = values.pop('sid')

@app.context_processor
async def inject_globals():
    from utils.menu_loader import load_menu_structure, filter_menu_by_permissions
    
    # Cargar estructura del menú
    menu_full = load_menu_structure()
    menu_filtered = filter_menu_by_permissions(menu_full, g.permissions) if hasattr(g, 'permissions') else {}
    
    return dict(
        current_user=g.user, 
        enterprise=g.get('enterprise'),
        permissions=g.permissions,
        csrf_token=session.get('csrf_token'),
        sid=g.get('sid', ''),
        now=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        menu_structure=menu_filtered
    )

@app.template_filter('format_currency')
def format_currency(value):
    try:
        return "{:,.2f}".format(float(value)).replace(',', 'X').replace('.', ',').replace('X', '.')
    except (ValueError, TypeError):
        return "0,00"

@app.template_filter('addslashes')
def addslashes_filter(s):
    if not isinstance(s, str):
        return s
    return s.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')

@app.template_filter('format_number')
def format_number(value):
    try:
        return "{:,}".format(int(value)).replace(',', '.')
    except (ValueError, TypeError):
        return str(value)

@app.template_filter('do_human_format')
def human_format(num):
    try:
        num = float(num)
        magnitude = 0
        while abs(num) >= 1000:
            magnitude += 1
            num /= 1000.0
        return '{}{}'.format('{:.1f}'.format(num).rstrip('0').rstrip('.'), ['', 'k', 'M', 'G', 'T', 'P'][magnitude])
    except:
        return num


from werkzeug.exceptions import HTTPException

@app.errorhandler(Exception)
async def global_exception_handler(e):
    # Atrapar cualquier error HTTP (400, 403, 404, 405, 500, etc) o RuntimeError, ValueError, etc.
    try:
        from database import get_db_cursor
        import json, traceback
        
        status_code = 500
        if isinstance(e, HTTPException):
            status_code = e.code
        
        # No loguear en DB los 404 de assets estáticos (ruido innecesario)
        STATIC_EXTENSIONS = ('.ico', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', 
                             '.css', '.js', '.woff', '.woff2', '.ttf', '.eot', '.map')
        if status_code == 404 and any(request.path.lower().endswith(ext) for ext in STATIC_EXTENSIONS):
            return '', 404
            
        req_data = {}
        try:
            if request.is_json: req_data = await request.json
            elif await request.form: req_data = dict(await request.form)
        except: pass
        
        clob = {
            'request_path': request.path,
            'referrer': request.referrer,
            'traceback': traceback.format_exc(),
            'exception_type': type(e).__name__
        }
        
        user_id = getattr(g, 'user', {}).get('id') if getattr(g, 'user', None) else None
        ent_id = getattr(g, 'user', {}).get('enterprise_id', 0) if getattr(g, 'user', None) else 0
        
        failure_mode = f"HTTP_{status_code}" if isinstance(e, HTTPException) else "UNHANDLED_EXCEPTION"
        error_msg = str(e) or 'Internal Server Error'

        try:
            import json, traceback
            
            sid = getattr(g, 'sid', None) or session.get('session_id')
            
            error_id = "N/A"
            try:
                async with get_db_cursor() as log_cursor:
                    await log_cursor.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'clob_data'")
                    has_clob = bool(await log_cursor.fetchone())
                    
                    if has_clob:
                        await log_cursor.execute("""
                            INSERT INTO sys_transaction_logs 
                            (enterprise_id, user_id, session_id, module, endpoint, request_method, request_data, 
                             status, severity, impact_category, failure_mode, error_message, clob_data)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (ent_id, user_id, sid, 'SYSTEM', request.path, request.method, json.dumps(req_data),
                              'ERROR', 8 if status_code >= 500 else 3, 'OPERACIONAL', failure_mode, error_msg, json.dumps(clob)))
                    else:
                        await log_cursor.execute("""
                            INSERT INTO sys_transaction_logs 
                            (enterprise_id, user_id, session_id, module, endpoint, request_method, request_data, 
                             status, severity, impact_category, failure_mode, error_message, error_traceback)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (ent_id, user_id, sid, 'SYSTEM', request.path, request.method, json.dumps(req_data),
                              'ERROR', 8 if status_code >= 500 else 3, 'OPERACIONAL', failure_mode, error_msg, json.dumps(clob)))
                    
                    error_id = log_cursor.lastrowid
            except Exception as log_ex:
                logger.error(f"Falla de registro en DB: {log_ex}")

            if status_code >= 500:
                try:
                    from services import email_service
                    
                    async def notify_superadmin(eid, emsg, tb):
                        async with app.app_context():
                            # Notificar caída general al super admin
                            success, err = await email_service._enviar_email(
                                "marcelocperi@gmail.com", 
                                f"[URGENTE] Caída General del Servicio - Error #{eid}", 
                                f"<h3>ALERTA DE SISTEMA CLASIFICADA</h3><p>Se ha registrado un error grave (HTTP 500+ o Exception crítica).</p><b>ID de Error:</b> #{eid}<br><b>Mensaje:</b> {emsg}<br><br><pre style='font-size:10px; background:#f4f4f4;'>{tb}</pre>",
                                enterprise_id=0
                            )
                            if success:
                                logger.info(f"✅ Email de alerta enviado exitosamente (Error #{eid})")
                            else:
                                logger.error(f"❌ FALLO al enviar email de alerta: {err}")

                    app.add_background_task(notify_superadmin, error_id, error_msg, traceback.format_exc())
                except Exception as mail_ex:
                    logger.error(f"Failed to trigger severe error email background task: {mail_ex}")
        except Exception as ex_inner:
            logger.error(f"Falla en lógica interna de global_handler: {ex_inner}")

    except Exception as ex:
        logger.error(f"Falla crítica registrando error en global_handler: {ex}")
    
    if isinstance(e, HTTPException):
        return e.description, e.code
    # Retornar el mensje de error exacto para que el frontend lo tome en el fetch:
    safe_error = str(e)
    if not safe_error or safe_error.strip() == '':
        safe_error = type(e).__name__
    return f"Falla interna del servidor: {safe_error}", 500

# def start_dns_updater():
#     """Inicia el actualizador de FreeDNS en un hilo separado."""
#     try:
#         from freedns_updater import run_updater
#         import threading
#         dns_thread = threading.Thread(target=run_updater, daemon=True)
#         dns_thread.start()
#         print("[INFO] Actualizador de FreeDNS iniciado en segundo plano.")
#     except Exception as e:
#         print(f"[ERROR] No se pudo iniciar el actualizador de DNS: {e}")

def ensure_port_is_free(port):
    """
    Detecta y cierra procesos que estén ocupando el puerto de red solicitado.
    Solo para Windows. Ayuda a 'curar en salud' contra procesos zombie (Chrome/Python).
    """
    import os
    import subprocess
    import signal
    
    if os.name != 'nt':
        return

    try:
        # Buscar el PID que escucha en el puerto
        cmd = f'netstat -ano | findstr LISTENING | findstr :{port}'
        output = subprocess.check_output(cmd, shell=True).decode()
        
        for line in output.strip().split('\n'):
            if f':{port}' in line and 'LISTENING' in line:
                pid = line.strip().split()[-1]
                if pid == '0' or int(pid) == os.getpid():
                    continue
                
                # Obtener nombre del proceso para el log
                try:
                    proc_info = subprocess.check_output(f'tasklist /fi "pid eq {pid}" /fo csv', shell=True).decode()
                    proc_name = proc_info.split('\n')[1].split(',')[0].strip('"')
                except:
                    proc_name = "Desconocido"
                
                logger.info(f"[AUTO-REMEDICACION] Puerto {port} ocupado por {proc_name} (PID: {pid}). Limpiando...")
                
                # Matar el proceso de forma forzada
                subprocess.call(f'taskkill /F /PID {pid}', shell=True)
                logger.info(f"[AUTO-REMEDICACION] Proceso {pid} desalojado exitosamente.")
    except Exception as e:
        # Si no hay nada en el puerto, check_output lanzará error, lo ignoramos
        pass

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    ensure_port_is_free(port)
    
    print("="*60)
    print("  COLOSAL ERP - SERVIDOR DE PRODUCCION (Quart/Hypercorn)")
    print("="*60)
    
    env = os.environ.get('FLASK_ENV', 'production').lower()

    if env == 'development':
        print(f"MODO: DESARROLLO (Quart Debug)")
        print(f"URL:  http://localhost:{port}")
        app.run(host='0.0.0.0', port=port, debug=True)
    else:
        try:
            from hypercorn.config import Config as HyperConfig
            from hypercorn.asyncio import serve
            import asyncio
            
            print(f"MODO: PRODUCCION (Hypercorn)")
            print(f"URL:  http://0.0.0.0:{port}")
            
            config = HyperConfig()
            config.bind = [f"0.0.0.0:{port}"]
            asyncio.run(serve(app, config))
        except ImportError:
            print("[ALERTA] Hypercorn no instalado. Usando Quart nativo.")
            app.run(host='0.0.0.0', port=port, debug=False)
