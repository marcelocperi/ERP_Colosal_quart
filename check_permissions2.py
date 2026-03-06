import os, ast, sys
sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
from database import get_db_cursor

blueprints_dir = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
modules = ['compras', 'ventas', 'fondos', 'stock', 'biblioteca', 'core', 'contabilidad', 'utilitarios']

all_permissions = set()
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SELECT id, code FROM sys_permissions")
    perms = cursor.fetchall()
    perm_dict = {p['id']: p['code'] for p in perms}
    for p in perms:
        all_permissions.add(p['code'])

resultados = []

for mod in modules:
    route_file = os.path.join(blueprints_dir, mod, 'routes.py')
    if not os.path.exists(route_file): continue
    
    with open(route_file, 'r', encoding='utf-8') as f:
        source = f.read()
        
    try:
         tree = ast.parse(source)
    except Exception as e:
         print(f"Error parsing {mod}: {e}")
         continue
         
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            permissions_required = []
            route_path = f"{mod}.{node.name}"
            
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call):
                    func_name = getattr(decorator.func, 'attr', getattr(decorator.func, 'id', ''))
                    if func_name == 'permission_required':
                        if decorator.args:
                            arg = decorator.args[0]
                            perm = getattr(arg, 'value', getattr(arg, 's', ''))
                            if perm: permissions_required.append(perm)
                            
            for perm in permissions_required:
                is_orphaned_in_profiles = perm not in all_permissions
                
                criticidad = "Alta / Crítica" if is_orphaned_in_profiles else "Baja"
                estado = "🔴 Huérfano en Perfiles" if is_orphaned_in_profiles else "🟢 Registrado"
                     
                resultados.append({
                    'modulo': mod.upper(),
                    'ruta': route_path,
                    'permiso': perm,
                    'huerfano': is_orphaned_in_profiles,
                    'estado': estado,
                    'criticidad': criticidad
                })

output = []
output.append("=== REPORTE AUDITORIA DE PERMISOS FANTASMAS (CODIGO VS DB) ===")

for mod in modules:
    mod_results = [r for r in resultados if r['modulo'] == mod.upper()]
    if not mod_results: continue
    
    total = len(mod_results)
    huerfanos = sum(1 for r in mod_results if r['huerfano'])
    
    if huerfanos > 0: estado_general = "🔴 Peligro (Hay permisos no declarados)"
    else: estado_general = "🟢 Seguro"
    
    crit_general = "Alta" if huerfanos > 0 else "Baja"
    output.append(f"| {mod.upper():<15} | Rutas con Permiso Explicíto: {total:<3} | Huérfanos: {huerfanos:<2} | Estado General: {estado_general:<35} | Criticidad C/R: {crit_general:<5} |")

output.append("\n=== DETALLE DE PERMISOS HUERFANOS (Bloquean Pantallas) ===")
for r in resultados:
    if r['huerfano']:
        output.append(f"Módulo {r['modulo']}: {r['ruta']} exige '{r['permiso']}' pero NO EXISTE EN LA BASE DE DATOS -> {r['estado']}")

with open(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\report_permissions2.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output))

print("Listo")
