import os
import json
import asyncio
import traceback
import aiomysql
from functools import wraps
from contextlib import asynccontextmanager
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

DB_CONFIG = {
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "3307")),
    "db": os.environ.get("DB_NAME", "multi_mcp_db"),
    "autocommit": True,
    "charset": "utf8mb4"
}

_async_pool = None

async def get_db_pool():
    global _async_pool
    if _async_pool is None:
        _async_pool = await aiomysql.create_pool(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            db=DB_CONFIG['db'],
            autocommit=DB_CONFIG['autocommit'],
            charset=DB_CONFIG['charset'],
            minsize=5,
            maxsize=int(os.environ.get("DB_POOL_SIZE", 32))
        )
        print(f"[OK] Async Connection Pool initialized (aiomysql).")
    return _async_pool

@asynccontextmanager
async def get_db_cursor(dictionary=False):
    """Context manager asíncrono para obtener un cursor con control de transacción."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Deshabilitamos autocommit temporalmente para esta conexión
        # para asegurar que el rollback() funcione si hay una excepción.
        await conn.autocommit(False)
        cursor_class = aiomysql.DictCursor if dictionary else aiomysql.Cursor
        async with conn.cursor(cursor_class) as cursor:
            try:
                yield cursor
                await conn.commit()
            except Exception as e:
                await conn.rollback()
                raise e
            finally:
                # Opcional: devolver autocommit a su estado por defecto si se desea reutilizar la conexión del pool
                await conn.autocommit(True)

def atomic_transaction(module='SYSTEM', severity=5, impact_category='OPERACIONAL', failure_mode='UNHANDLED_EXCEPTION'):
    """Decorador asíncrono para transacciones atómicas en Quart."""
    def decorator(f):
        @wraps(f)
        async def async_wrapper(*args, **kwargs):
            from quart import g, has_request_context
            user_id = None
            ent_id = 0
            if has_request_context():
                user_id = getattr(g, 'user', {}).get('id') if hasattr(g, 'user') and g.user else None
                ent_id = getattr(g, 'user', {}).get('enterprise_id', 0) if hasattr(g, 'user') and g.user else 0
            
            try:
                # Nota: En aiomysql el pool maneja la conexión. 
                # Si f() usa get_db_cursor(), ya está en el pool.
                return await f(*args, **kwargs)
            except Exception as e:
                await _log_transaction_error(e, ent_id, user_id, module, severity, impact_category, failure_mode)
                raise e
        return async_wrapper
    return decorator

async def _log_transaction_error(e, ent_id, user_id, module, severity, impact_category, failure_mode):
    from quart import request, has_request_context, session, g
    try:
        req_path = request.path if has_request_context() else 'CLI/CRON'
        req_meth = request.method if has_request_context() else 'N/A'
        req_data = {}
        if has_request_context():
            try:
                if request.is_json:
                    req_data = await request.get_json(silent=True) or {}
                else:
                    # El body puede ya haber sido consumido por la ruta. Silenciar el error.
                    form = await request.form
                    req_data = dict(form)
            except Exception:
                pass  # Body ya consumido, no bloquear el flujo
        
        clob = {
            'request_path': req_path,
            'referrer': request.referrer if has_request_context() else None,
            'traceback': traceback.format_exc(),
            'exception_type': type(e).__name__
        }
        
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
            """, (ent_id, user_id, sid, module, req_path, req_meth, json.dumps(req_data),
                  'ERROR', severity, impact_category, failure_mode, str(e), json.dumps(clob)))
    except Exception as log_ex:
        print(f"[CRITICAL] Error en logger: {log_ex}")

# Mantenemos init_db síncrono para el arranque inicial (usando pymysql)
def init_db():
    import pymysql
    try:
        config = {
            "host": DB_CONFIG['host'],
            "port": DB_CONFIG['port'],
            "user": DB_CONFIG['user'],
            "password": DB_CONFIG['password'],
            "database": DB_CONFIG['db'],
            "connect_timeout": 5
        }
        conn = pymysql.connect(**config)
        conn.close()
        return True
    except Exception as e:
        print(f"[WARNING] init_db falló: {e}")
        return False
