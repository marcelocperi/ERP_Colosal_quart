import os
import sys
import json
import traceback
from functools import wraps
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Detector de Tipo de Base de Datos
DB_TYPE = os.environ.get("DB_TYPE", "mariadb").lower()
Base = declarative_base()

# ⚠️ SEGURIDAD: Nunca usar valores por defecto para credenciales en producción
def get_required_env(key, default=None):
    """Obtiene variable de entorno requerida, falla si no existe en producción."""
    value = os.environ.get(key, default)
    
    # En producción, no permitir defaults para credenciales
    if os.environ.get("FLASK_ENV") == "production" and default and key in ["DB_PASSWORD", "FLASK_SECRET_KEY"]:
        if value == default:
            raise ValueError(f"[SECURITY] {key} debe ser configurada en producción, no usar default")
    
    return value

# Configuración centralizada de la Base de Datos
DB_CONFIG = {
    "user": get_required_env("DB_USER", "root"),
    "password": get_required_env("DB_PASSWORD"),  # ⚠️ SIN DEFAULT - debe venir de .env
    "host": get_required_env("DB_HOST", "localhost"),
    "port": int(get_required_env("DB_PORT", "3307")),
    "database": get_required_env("DB_NAME", "multi_mcp_db"),
    "connect_timeout": 10,  # FIX FREEZE: evita bloqueos indefinidos en MariaDB pool
    "init_command": "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
}

# Validar que password no esté vacía
if not DB_CONFIG["password"]:
    raise ValueError("[SECURITY] DB_PASSWORD no puede estar vacía. Configurar en archivo .env")

# Driver Import Logic
try:
    if DB_TYPE == 'sqlite':
        import sqlite3
    elif DB_TYPE == 'sqlserver':
        try:
            import pymssql
        except ImportError:
            import pyodbc
    else:
        if DB_TYPE == 'mysql':
            import pymysql
            mariadb = None
        else:
            try:
                import mariadb
            except ImportError:
                import pymysql
                mariadb = None
except ImportError as e:
    print(f"[WARNING] Advertencia: Driver para {DB_TYPE} no encontrado: {e}")

# --- SQLAlchemy Configuration ---
_engine = None
_SessionLocal = None

def get_engine():
    global _engine
    if _engine is None:
        if DB_TYPE == 'mariadb' or DB_TYPE == 'mysql':
            driver = "mariadb" if mariadb else "pymysql"
            connection_url = f"mysql+{driver}://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        elif DB_TYPE == 'sqlserver':
            # Intentar pymssql primero (más portable)
            connection_url = f"mssql+pymssql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        elif DB_TYPE == 'sqlite':
            connection_url = f"sqlite:///{os.environ.get('DB_NAME', 'multi_mcp.db')}"
        else:
            raise ValueError(f"Unsupported DB_TYPE: {DB_TYPE}")
        
        _engine = create_engine(connection_url, pool_pre_ping=True, pool_recycle=3600)
    return _engine

def get_session():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    return _SessionLocal()

# --- Legacy Connection Pool (MariaDB/MySQL) ---
_db_pool = None

def get_db_pool():
    global _db_pool
    if _db_pool is None:
        if not mariadb:
            return None # No hay pool nativo para pymysql, usar conexiones directas
            
        # FIX: Evitar el freeze de Werkzeug Reloader al modificar archivos python
        is_dev = os.environ.get('FLASK_ENV', '').lower() == 'development' or os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
        if is_dev:
            # El pool en C de MariaDB bloquea el hilo principal y cuelga la app en los hot-reloads.
            return None

        try:
            # Crear pool con nombre único para evitar conflictos
            pool_name = os.environ.get("DB_POOL_NAME", "web_app_pool")
            
            # EL TAMAÑO DEL POOL NO ES LA CANTIDAD DE USUARIOS
            # En la web un request a la BD dura menos de 1 milisegundo. 
            # Un pool de 32 (hilos concurrentes directos a disco) atiende sin problema sobre 2,000 usuarios conectados en tiempo real.
            # Se setea el default en 32 para escalabilidad en producción (Waitress y Gunicorn manejan entre 4 y 32 workers).
            pool_size = int(os.environ.get("DB_POOL_SIZE", 32))
            
            # mariadb.ConnectionPool no acepta init_command ni connect_timeout — filtrar ambos
            POOL_UNSUPPORTED_KEYS = ('init_command', 'connect_timeout')
            config = {k: v for k, v in DB_CONFIG.items() if k not in POOL_UNSUPPORTED_KEYS}
            _db_pool = mariadb.ConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                **config
            )
            print(f"[OK] Connection Pool '{pool_name}' initialized (size={pool_size}) para alta escalabilidad.")
        except mariadb.Error as e:
            print(f"[WARNING] Error initializing Connection Pool: {e}")
            raise e
    return _db_pool

@contextmanager
def get_db_cursor(dictionary=False):
    """Context manager para obtener un cursor desde el pool."""
    conn = None
    try:
        if DB_TYPE == 'sqlite':
            conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            if dictionary: conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        else:
            # Obtener conexión
            pool = get_db_pool()
            if pool:
                conn = pool.get_connection()
                cursor = conn.cursor(dictionary=dictionary)
            else:
                # Fallback a on-the-fly connections
                config = {k: v for k, v in DB_CONFIG.items() if k != 'init_command'}
                if mariadb:
                    conn = mariadb.connect(**config)
                    cursor = conn.cursor(dictionary=dictionary)
                else:
                    conn = pymysql.connect(**config)
                    cursor = conn.cursor(pymysql.cursors.DictCursor if dictionary else None)
            yield cursor
            conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        # Redactar password en logs
        safe_error = str(e)
        if DB_CONFIG.get("password") and DB_CONFIG["password"] in safe_error:
            safe_error = safe_error.replace(DB_CONFIG["password"], "***REDACTED***")
        print(f"Error Database ({DB_TYPE}): {safe_error}")
        raise e
    finally:
        # En pool, close() devuelve la conexión al pool
        if conn: conn.close()

import inspect

def atomic_transaction(module='SYSTEM', severity=5, impact_category='OPERACIONAL', failure_mode='UNHANDLED_EXCEPTION'):
    """
    Decorador para envolver funciones en una transacción atómica con logging de errores.
    Soporta funciones síncronas y asíncronas (async/await).
    """
    def decorator(f):
        if inspect.iscoroutinefunction(f):
            @wraps(f)
            async def async_wrapper(*args, **kwargs):
                from flask import g, request, has_request_context
                user_id = None
                ent_id = 0
                if has_request_context():
                    user_id = getattr(g, 'user', {}).get('id') if hasattr(g, 'user') and g.user else None
                    ent_id = getattr(g, 'user', {}).get('enterprise_id', 0) if hasattr(g, 'user') and g.user else 0
                
                try:
                    return await f(*args, **kwargs)
                except Exception as e:
                    _log_transaction_error(e, ent_id, user_id, module, severity, impact_category, failure_mode)
                    raise e
            return async_wrapper
        else:
            @wraps(f)
            def sync_wrapper(*args, **kwargs):
                from flask import g, request, has_request_context
                user_id = None
                ent_id = 0
                if has_request_context():
                    user_id = getattr(g, 'user', {}).get('id') if hasattr(g, 'user') and g.user else None
                    ent_id = getattr(g, 'user', {}).get('enterprise_id', 0) if hasattr(g, 'user') and g.user else 0
                
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    _log_transaction_error(e, ent_id, user_id, module, severity, impact_category, failure_mode)
                    raise e
            return sync_wrapper
    return decorator

def _log_transaction_error(e, ent_id, user_id, module, severity, impact_category, failure_mode):
    """Helper interno para loguear errores de transacciones."""
    from flask import request, has_request_context
    try:
        req_path = request.path if has_request_context() else 'CLI/CRON'
        req_meth = request.method if has_request_context() else 'N/A'
        req_data = {}
        if has_request_context():
            try:
                if request.is_json: req_data = request.json
                elif request.form: req_data = dict(request.form)
            except: pass
        
        clob = {
            'request_path': req_path,
            'referrer': request.referrer if has_request_context() else None,
            'traceback': traceback.format_exc(),
            'exception_type': type(e).__name__
        }
        
        from flask import session, g
        sid = getattr(g, 'sid', None) or session.get('session_id')
        
        with get_db_cursor() as log_cursor:
            log_cursor.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'clob_data'")
            has_clob = bool(log_cursor.fetchone())
            col = 'clob_data' if has_clob else 'error_traceback'
            
            log_cursor.execute(f"""
                INSERT INTO sys_transaction_logs 
                (enterprise_id, user_id, session_id, module, endpoint, request_method, request_data, 
                 status, severity, impact_category, failure_mode, error_message, {col})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (ent_id, user_id, sid, module, req_path, req_meth, json.dumps(req_data),
                  'ERROR', severity, impact_category, failure_mode, str(e), json.dumps(clob)))
    except Exception as log_ex:
        print(f"[CRITICAL] Falló el log de error en sys_transaction_logs: {log_ex}")

def init_db():
    """Verificación inicial de la conexión."""
    try:
        if DB_TYPE == 'sqlite':
            conn = sqlite3.connect(DB_PATH)
            conn.close()
            return True
        else:
            config = {k: v for k, v in DB_CONFIG.items() if k != 'init_command'}
            if mariadb:
                conn = mariadb.connect(**DB_CONFIG)
            elif pymysql:
                conn = pymysql.connect(**config)
            else:
                return False
            conn.close()
            return True
    except Exception as e:
        print(f"[WARNING] Error al conectar a la base de datos: {e}")
        return False


def calcular_proxima_ejecucion(frecuencia, planificacion_str):
    """
    Calcula la próxima fecha/hora de ejecución a partir de ahora según la planificación.
    Usada por los scripts de cron para persistir proxima_ejecucion en sys_crons.
    """
    from datetime import datetime, timedelta
    try:
        now = datetime.now()
        plan = json.loads(planificacion_str) if isinstance(planificacion_str, str) else planificacion_str
        if not plan:
            return None

        if frecuencia == 'semanal' and plan.get('days'):
            days = [int(d) for d in plan['days']]  # 1=Lun ... 7=Dom
            hour, minute = map(int, plan.get('hour', '00:00').split(':'))
            for offset in range(1, 9):  # Siempre la PRÓXIMA, no hoy
                candidate = now + timedelta(days=offset)
                if candidate.isoweekday() in days:
                    return candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)

        elif frecuencia == 'diaria' and plan.get('hour'):
            hour, minute = map(int, plan['hour'].split(':'))
            candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(days=1)
            return candidate

        elif frecuencia == 'minutos' and plan.get('minutes'):
            return now + timedelta(minutes=int(plan['minutes']))

    except Exception as e:
        print(f"[WARNING] calcular_proxima_ejecucion error: {e}")
    return None
