import os, ast, json, sys

blueprints_dir = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
modules = ['compras', 'ventas', 'fondos', 'stock', 'biblioteca', 'core', 'contabilidad', 'utilitarios']
menu_file = os.path.join(blueprints_dir, '.agent', 'menu_structure.json')

with open(menu_file, 'r', encoding='utf-8') as f:
    menu = json.load(f)

menu_routes = set()
for cat_name, cat_data in menu['menu_tree'].items():
    for module in cat_data.get('modules', []):
        menu_routes.add(module.get('route', ''))

output = []
output.append("=== RUTAS HURFANAS (EN CODIGO, PERO SIN ENLACE EN EL MENU) ===")

count = 0
for mod in modules:
    route_file = os.path.join(blueprints_dir, mod, 'routes.py')
    if not os.path.exists(route_file): continue
    
    with open(route_file, 'r', encoding='utf-8') as f:
        source = f.read()
        
    try:
        tree = ast.parse(source)
    except Exception as e:
        output.append(f"Error parsing {mod}: {e}")
        continue
        
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Call) and getattr(decorator.func, 'attr', '') == 'route':
                    route_path = ''
                    if decorator.args:
                        arg = decorator.args[0]
                        route_path = getattr(arg, 'value', getattr(arg, 's', ''))
                    
                    methods = ['GET']
                    for kw in decorator.keywords:
                        if kw.arg == 'methods':
                            methods = []
                            for el in kw.value.elts:
                                val = getattr(el, 'value', getattr(el, 's', ''))
                                if val: methods.append(val)
                    
                    is_main_view = 'GET' in methods and '<' not in route_path and not route_path.startswith('/api/') and 'post_' not in node.name and not route_path.endswith('/create') and not route_path.endswith('/nueva') and not route_path.endswith('/nuevo') and 'login' not in node.name and 'logout' not in node.name and 'reset' not in node.name
                    
                    if is_main_view:
                        endpoint = f"{mod}.{node.name}"
                        if endpoint not in menu_routes:
                            output.append(f"{endpoint:<35} -> {route_path}")
                            count += 1
                            
output.append(f"Total posibles desconectadas: {count}")

with open(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\report_orphans.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output))
