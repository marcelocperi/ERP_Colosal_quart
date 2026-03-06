import os
import sys
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
            raise ValueError(f"⚠️ SECURITY: {key} debe ser configurada en producción, no usar default")
    
    return value

# Configuración centralizada de la Base de Datos
DB_CONFIG = {
    "user": get_required_env("DB_USER", "root"),
    "password": get_required_env("DB_PASSWORD"),  # ⚠️ SIN DEFAULT - debe venir de .env
    "host": get_required_env("DB_HOST", "localhost"),
    "port": int(get_required_env("DB_PORT", "3307")),
    "database": get_required_env("DB_NAME", "multi_mcp_db"),
    "init_command": "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
}

# Validar que password no esté vacía
if not DB_CONFIG["password"]:
    raise ValueError("⚠️ SECURITY: DB_PASSWORD no puede estar vacía. Configurar en archivo .env")

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
    print(f"⚠️ Advertencia: Driver para {DB_TYPE} no encontrado: {e}")

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
        try:
            # Crear pool con nombre único para evitar conflictos
            pool_name = os.environ.get("DB_POOL_NAME", "web_app_pool")
            pool_size = int(os.environ.get("DB_POOL_SIZE", 5))
            
            _db_pool = mariadb.ConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                **DB_CONFIG
            )
            print(f"✅ Connection Pool '{pool_name}' initialized (size={pool_size})")
        except mariadb.Error as e:
            print(f"⚠️ Error initializing Connection Pool: {e}")
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
                # Fallback a pymysql manual
                config = {k: v for k, v in DB_CONFIG.items() if k != 'init_command'}
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
        print(f"⚠️ Error al conectar a la base de datos: {e}")
        return False
