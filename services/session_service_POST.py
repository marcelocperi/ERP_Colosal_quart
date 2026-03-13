
from quart import request, session, g
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
    async def get_current_sid():
        """
        Deduce el SID (Session ID) priorizando el contexto explícito de la pestaña.
        """
        # 1. Ya asignado en g por preprocessor
        sid = getattr(g, 'sid', None)
        
        # 2. De los parámetros del request
        if not sid:
            sid = request.args.get('sid')
            if not sid:
                try:
                    form_data = await request.form
                    sid = form_data.get('sid')
                except:
                    pass
            if not sid:
                sid = request.headers.get('X-SID')
            
        # 3. Guardar en g para uso futuro en este request
        if sid:
            g.sid = sid
            
        return sid

    @staticmethod
    async def attach_session_context():
        """Puebla 'g' con la identidad basada en el SID de la pestaña."""
        g.user = None
        g.permissions = []
        g.enterprise = None
        
        sid = await SessionDispatcher.get_current_sid()
        
        with open('login_debug.txt', 'a') as f: f.write(f"-> ATTACH_CONTEXT: sid_requested={sid}, session_keys={list(session.keys())}\n")
        
        if 's' not in session:
            with open('login_debug.txt', 'a') as f: f.write("   -> WARNING: 's' not in session object!\n")
            session['s'] = {}
            # Si el usuario está logueado pero perdió el sid (ej: refresco manual), 
            # podríamos intentar recuperar el último, pero es más seguro obligar a uno nuevo.
        
        if sid and sid in session['s']:
            ctx = session['s'][sid]
            uid = ctx.get('user_id')
            eid = ctx.get('enterprise_id')
            with open('login_debug.txt', 'a') as f: f.write(f"   -> FOUND IN SESSION: uid={uid}, eid={eid}\n")
            if uid is not None and eid is not None:
                await SessionDispatcher._load_full_context(uid, eid)
        elif not sid and 'user_id' in session and 'enterprise_id' in session:
            # Caso: Usuario logueado (sesión clásica) pero sin pestaña definida.
            # Generamos un SID por defecto para esta "pestaña huérfana".
            new_sid = secrets.token_hex(4)
            g.sid = new_sid
            session['s'][new_sid] = {
                'user_id': session['user_id'],
                'enterprise_id': session['enterprise_id'],
                'last_activity': time.time()
            }
            session.modified = True
            await SessionDispatcher._load_full_context(session['user_id'], session['enterprise_id'])

    @staticmethod
    async def _load_full_context(user_id, ent_id):
        """Carga datos optimizados de usuario, empresa y permisos con soporte de caché."""
        now_ts = time.time()
        cache_key = (ent_id, user_id)
        
        # 1. Caché Layer
        with PERMISSION_LOCK:
            if cache_key in PERMISSION_CACHE:
                ts, cached_user, cached_perms, cached_ent = PERMISSION_CACHE[cache_key]
                if now_ts - ts < CACHE_TTL:
                    with open('login_debug.txt', 'a') as f: f.write(f"   -> CACHE HIT: g.user set to {cached_user}\n")
                    # Copias defensivas: evitar que mutaciones de g contaminen el caché
                    g.user = dict(cached_user) if cached_user else cached_user
                    g.permissions = list(cached_perms)
                    g.enterprise = dict(cached_ent) if cached_ent else cached_ent
                    return
                else:
                    with open('login_debug.txt', 'a') as f: f.write(f"   -> CACHE EXPIRED\n")

        # 2. Database Layer
        try:
            async with get_db_cursor(dictionary=True) as cursor:
                # Carga de Usuario y Rol (Query optimizada)
                await cursor.execute("""
                    SELECT u.id, u.username, r.name as role_name, u.role_id
                    FROM sys_users u 
                    LEFT JOIN sys_roles r ON u.role_id = r.id AND r.enterprise_id = u.enterprise_id
                    WHERE u.id = %s AND u.enterprise_id = %s
                """, (user_id, ent_id))
                user_row = await cursor.fetchone()
                
                if not user_row:
                    with open('login_debug.txt', 'a') as f: f.write(f"   -> ERROR: No user_row found for uid={user_id}, eid={ent_id}\n")
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
                await cursor.execute("SELECT nombre, logo_path, lema FROM sys_enterprises WHERE id = %s", (ent_id,))
                ent_row = await cursor.fetchone()
                g.enterprise = ent_row if ent_row else {'nombre': 'Gestión Corporativa', 'logo_path': None}

                # Carga de Permisos (Consolidado)
                await cursor.execute("""
                    SELECT DISTINCT p.code 
                    FROM sys_permissions p
                    JOIN sys_role_permissions rp ON p.id = rp.permission_id
                    WHERE rp.role_id = %s AND rp.enterprise_id = %s
                """, (user_row['role_id'], ent_id))
                g.permissions = [str(row['code']).lower().strip() for row in await cursor.fetchall()]

                # --- Administración y Bypasses de Seguridad ---
                with open('login_debug.txt', 'a') as f: f.write(f"   -> DB SUCCESS: g.user set to {g.user}\n")
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
        
        except Exception as e:
            with open('login_debug.txt', 'a') as f: f.write(f"   -> EXCEPTION in _load_full_context: {e}\n")
            import traceback
            with open('login_debug.txt', 'a') as f: f.write(traceback.format_exc() + "\n")
            raise

    @staticmethod
    def invalidate_cache(user_id, enterprise_id):
        """Limpia la caché de un usuario específico cuando cambian sus permisos."""
        with PERMISSION_LOCK:
            PERMISSION_CACHE.pop((enterprise_id, user_id), None)
