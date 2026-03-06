import os, ast, re, sys
sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
from database import get_db_cursor

blueprints_dir = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
modules = ['compras', 'ventas', 'fondos', 'stock', 'biblioteca', 'core', 'contabilidad', 'utilitarios']

# 1. Obtenemos permisos del sistema y roles asignados desde la DB
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

output = ["=== AUDITORIA DE PERMISOS: CODIGO VS ROLES ASIGNABLES ==="]
output.append(f"Total permisos inscriptos en BD (sys_permissions): {len(all_permissions)}")
output.append(f"Total permisos con al menos un rol asignado: {len(permissions_with_roles)}\n")

resultados = []

for mod in modules:
    route_file = os.path.join(blueprints_dir, mod, 'routes.py')
    if not os.path.exists(route_file): continue
    
    with open(route_file, 'r', encoding='utf-8') as f:
        source = f.read()
        
    try:
         tree = ast.parse(source)
    except Exception as e:
         continue
         
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            route_path = ""
            permission = None
            
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call):
                    func_name = getattr(decorator.func, 'attr', getattr(decorator.func, 'id', ''))
                    if func_name == 'route':
                        if decorator.args:
                            arg = decorator.args[0]
                            route_path = getattr(arg, 'value', getattr(arg, 's', ''))
                    elif func_name == 'permission_required':
                        if decorator.args:
                            arg = decorator.args[0]
                            permission = getattr(arg, 'value', getattr(arg, 's', ''))
            
            if route_path and permission:
                is_orphaned_in_profiles = permission not in all_permissions
                has_roles = permission in permissions_with_roles
                
                criticidad = "Alta" if is_orphaned_in_profiles else ("Media" if not has_roles else "Baja")
                if is_orphaned_in_profiles:
                     estado = "🔴 Huérfano en Perfiles"
                elif not has_roles:
                     estado = "🟡 Sin Roles Asignados"
                else:
                     estado = "🟢 Ok"
                     
                resultados.append({
                    'modulo': mod.upper(),
                    'ruta': route_path,
                    'permiso': permission,
                    'huerfano': is_orphaned_in_profiles,
                    'tiene_roles': has_roles,
                    'estado': estado,
                    'criticidad': criticidad
                })

# Grouping results by module
for mod in modules:
    mod_results = [r for r in resultados if r['modulo'] == mod.upper()]
    if not mod_results: continue
    
    total = len(mod_results)
    huerfanos = sum(1 for r in mod_results if r['huerfano'])
    sin_roles = sum(1 for r in mod_results if not r['tiene_roles'] and not r['huerfano'])
    
    if huerfanos > 0: estado_general = "🔴 Peligro (Hay permisos no declarados)"
    elif sin_roles > 0: estado_general = "🟡 Riesgo (Permisos sin usuarios)"
    else: estado_general = "🟢 Seguro"
    
    crit_general = "Alta" if huerfanos > 0 else ("Media" if sin_roles > 0 else "Baja")
    output.append(f"| {mod.upper():<15} | Endpoints Protegidos: {total:<3} | Huérfanos: {huerfanos:<2} | Sin Rol: {sin_roles:<2} | Estado: {estado_general:<35} | Criticidad: {crit_general:<5} |")

output.append("\n=== DETALLE DE PERMISOS HUERFANOS/SIN ROL ===")
for r in resultados:
    if r['huerfano'] or not r['tiene_roles']:
        output.append(f"Módulo {r['modulo']}: {r['ruta']} -> Requiere '{r['permiso']}' -> {r['estado']}")

with open(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\report_permissions.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output))

print("Done. Details in report_permissions.txt")
