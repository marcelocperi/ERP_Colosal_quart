import os, json, sys
sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
from database import get_db_cursor

blueprints_dir = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
menu_file = os.path.join(blueprints_dir, '.agent', 'menu_structure.json')

with open(menu_file, 'r', encoding='utf-8') as f:
    menu = json.load(f)

# Obtenemos permisos de la BD
all_permissions = set()  # Permisos que existen en sys_permissions
permissions_with_roles = set() # Permisos que algun rol tiene

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT id, code FROM sys_permissions")
    perms = cursor.fetchall()
    perm_dict = {p['id']: p['code'] for p in perms}
    for p in perms:
        all_permissions.add(p['code'])
        
    cursor.execute("SELECT DISTINCT permission_id FROM sys_role_permissions")
    for r in cursor.fetchall():
        if r['permission_id'] in perm_dict:
            permissions_with_roles.add(perm_dict[r['permission_id']])

# Buscamos permisos requeridos por el MENÚ
resultados = []

for mod_name, cat_data in menu['menu_tree'].items():
    for module in cat_data.get('modules', []):
        perm_required = module.get('permission', 'all')
        if perm_required == 'all':
             continue
             
        route = module.get('route', 'N/A')
        
        is_orphaned_in_profiles = perm_required not in all_permissions
        has_roles = perm_required in permissions_with_roles
        
        criticidad = "Alta" if is_orphaned_in_profiles else ("Media" if not has_roles else "Baja")
        estado = "🔴 Huérfano en Perfiles" if is_orphaned_in_profiles else ("🟡 Módulo fantasma (sin roles asignados a este permiso)" if not has_roles else "🟢 Asignable y con Roles")
             
        resultados.append({
            'categoria': mod_name.upper(),
            'modulo': module.get('name'),
            'ruta': route,
            'permiso': perm_required,
            'huerfano': is_orphaned_in_profiles,
            'tiene_roles': has_roles,
            'estado': estado,
            'criticidad': criticidad
        })

output = []
output.append("=== AUDITORIA DE PERMISOS: MENU VS BASE DE DATOS DE ROLES ===")

categorias = set(r['categoria'] for r in resultados)

for cat in categorias:
    cat_results = [r for r in resultados if r['categoria'] == cat]
    total = len(cat_results)
    huerfanos = sum(1 for r in cat_results if r['huerfano'])
    sin_roles = sum(1 for r in cat_results if not r['tiene_roles'] and not r['huerfano'])
    
    if huerfanos > 0: estado_general = "🔴 Peligro Falla UI/SoD"
    elif sin_roles > 0: estado_general = "🟡 Inaccesible en UI (Nadie lo tiene)"
    else: estado_general = "🟢 Seguro"
    
    crit_general = "Crítica" if huerfanos > 0 else ("Media" if sin_roles > 0 else "Baja")
    output.append(f"| {cat:<15} | Módulos En UI: {total:<3} | Huérfanos en Perfiles: {huerfanos:<2} | Sin Rol(Bloquea): {sin_roles:<2} | Estado: {estado_general:<35} | Criticidad: {crit_general:<10} |")

output.append("\n=== DETALLE DE PERMISOS ANOMALOS O FALTANTES ===")
for r in resultados:
    if r['huerfano'] or not r['tiene_roles']:
        output.append(f"[{r['categoria']}] '{r['modulo']}' ({r['ruta']}) -> PIDE: '{r['permiso']}' -> {r['estado']}")

with open(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\report_menu_perms.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output))

print("Listo")
