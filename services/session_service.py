
from flask import request, session, g
import time
import logging
import threading
import secrets
from database import get_db_cursor

logger = logging.getLogger(__name__)

# Cache global para permisos y datos de usuario (Thread-safe)
PERMISSION_CACHE = {} 
PERMISSION_LOCK = threading.Lock()
CACHE_TTL = 300  # 5 minutos

class SessionDispatcher:
    """
    Dispatcher centralizado para la gestión de identidades y contextos de usuario.
    Permite estabilidad ante la pérdida de parámetros en la URL y soporta 
    múltiples usuarios en distintas pestañas mediante la cookie pool.
    """
    
    @staticmethod
    def get_current_sid():
        """
        Deduce el SID (Session ID) priorizando el contexto explícito de la pestaña 
        y fallando elegantemente hacia el pool de la cookie.
        """
        # Prioridad 1: Contexto ya extraído por preprocesadores de URL o Headers
        sid = getattr(g, 'sid', None)
        
        # Prioridad 2: Parámetros explícitos en la petición (Query String / Form / Header)
        if not sid:
            sid = request.args.get('sid') or request.form.get('sid') or request.headers.get('X-SID')
        
        # ELIMINADO: Fallback de recuperación automática. 
        # Para preservar la independencia de pestañas, no debemos 'adivinar' la sesión
        # si el parámetro no viene explícitamente en la URL.
            
        return sid

    @staticmethod
    def attach_session_context():
        """
        Puebla el objeto global 'g' con la identidad del usuario, permisos y empresa.
        Es 'mudo' (no redirige) para permitir que los decoradores o módulos 
        manejen la ausencia de sesión según su necesidad.
        """
        # Inicialización segura de g
        g.user = None
        g.permissions = []
        g.enterprise = None
        # No reseteamos g.sid aquí por si viene de url_value_preprocessor
        
        if 's' not in session:
            session['s'] = {}
            return

        sid = SessionDispatcher.get_current_sid()
        g.sid = sid
        
        instance_session = session['s'].get(sid) if sid else None
        if not instance_session:
            # Si el SID entregado no existe, intentamos limpiar o registrar el fallo
            return

        user_id = instance_session.get('user_id')
        ent_id = instance_session.get('enterprise_id')
        
        if user_id is not None and ent_id is not None:
            try:
                SessionDispatcher._load_full_context(user_id, ent_id)
            except Exception as e:
                logger.error(f"Falla crítica cargando contexto de sesión: {e}")

    @staticmethod
    def _load_full_context(user_id, ent_id):
        """Carga datos optimizados de usuario, empresa y permisos con soporte de caché."""
        now_ts = time.time()
        cache_key = (ent_id, user_id)
        
        # 1. Caché Layer
        with PERMISSION_LOCK:
            if cache_key in PERMISSION_CACHE:
                ts, cached_user, cached_perms, cached_ent = PERMISSION_CACHE[cache_key]
                if now_ts - ts < CACHE_TTL:
                    # Copias defensivas: evitar que mutaciones de g contaminen el caché
                    g.user = dict(cached_user) if cached_user else cached_user
                    g.permissions = list(cached_perms)
                    g.enterprise = dict(cached_ent) if cached_ent else cached_ent
                    return

        # 2. Database Layer
        with get_db_cursor(dictionary=True) as cursor:
            # Carga de Usuario y Rol (Query optimizada)
            cursor.execute("""
                SELECT u.id, u.username, r.name as role_name, u.role_id
                FROM sys_users u 
                LEFT JOIN sys_roles r ON u.role_id = r.id AND r.enterprise_id = u.enterprise_id
                WHERE u.id = %s AND u.enterprise_id = %s
            """, (user_id, ent_id))
            user_row = cursor.fetchone()
            
            if not user_row:
                return

            role_clean = (user_row['role_name'] or 'Sin Rol').strip()
            g.user = {
                'id': user_row['id'],
                'username': user_row['username'],
                'role_name': role_clean,
                'role_id': user_row['role_id'],
                'enterprise_id': ent_id
            }

            # Carga de Datos de Empresa
            cursor.execute("SELECT nombre, logo_path, lema FROM sys_enterprises WHERE id = %s", (ent_id,))
            ent_row = cursor.fetchone()
            g.enterprise = ent_row if ent_row else {'nombre': 'Gestión Corporativa', 'logo_path': None}

            # Carga de Permisos (Consolidado)
            cursor.execute("""
                SELECT DISTINCT p.code 
                FROM sys_permissions p
                JOIN sys_role_permissions rp ON p.id = rp.permission_id
                WHERE rp.role_id = %s AND rp.enterprise_id = %s
            """, (user_row['role_id'], ent_id))
            g.permissions = [str(row['code']).lower().strip() for row in cursor.fetchall()]

            # --- Administración y Bypasses de Seguridad ---
            username_lower = str(user_row['username']).lower()
            role_lower = role_clean.lower()
            
            # SysAdmin Nivel Master
            if username_lower == 'superadmin' or role_lower == 'adminsys':
                if 'all' not in g.permissions: g.permissions.append('all')
                if 'sysadmin' not in g.permissions: g.permissions.append('sysadmin')
            
            # Administrador local de Empresa
            if username_lower == 'admin' or role_lower in ['admin', 'administrador', 'administrator'] or user_id == 1:
                if 'all' not in g.permissions: g.permissions.append('all')

            # --- Guardado en Caché (copias defensivas para evitar mutaciones futuras) ---
            with PERMISSION_LOCK:
                PERMISSION_CACHE[cache_key] = (now_ts, dict(g.user), list(g.permissions), dict(g.enterprise) if g.enterprise else g.enterprise)

    @staticmethod
    def invalidate_cache(user_id, enterprise_id):
        """Limpia la caché de un usuario específico cuando cambian sus permisos."""
        with PERMISSION_LOCK:
            PERMISSION_CACHE.pop((enterprise_id, user_id), None)
