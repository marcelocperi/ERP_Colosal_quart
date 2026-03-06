import ast, re, os
from collections import defaultdict

blueprints_dir = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
modules = ['compras', 'ventas', 'fondos', 'stock', 'biblioteca', 'core', 'contabilidad', 'utilitarios']

print("=" * 70)
print("AUDIT: Duplicate function names (=> Flask silently binds last one)")
print("=" * 70)

for mod in modules:
    route_file = os.path.join(blueprints_dir, mod, 'routes.py')
    if not os.path.exists(route_file):
        print(f'[{mod}]  NO routes.py found')
        continue
    with open(route_file, 'r', encoding='utf-8') as f:
        source = f.read()
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        print(f'[{mod}]  SYNTAX ERROR: {e}')
        continue
    func_lines = defaultdict(list)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            func_lines[node.name].append(node.lineno)
    dups = {n: l for n, l in func_lines.items() if len(l) > 1}
    if dups:
        print(f'[{mod}]  DUPLICATE FUNCTIONS: {dups}')
    else:
        print(f'[{mod}]  OK')

print()
print("=" * 70)
print("AUDIT: Explicit endpoint= collisions (same name registered twice)")
print("=" * 70)

for mod in modules:
    route_file = os.path.join(blueprints_dir, mod, 'routes.py')
    if not os.path.exists(route_file):
        continue
    with open(route_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    endpoints = defaultdict(list)
    for i, line in enumerate(lines, 1):
        m = re.search(r"endpoint=['\"]([^'\"]+)['\"]", line)
        if m:
            endpoints[m.group(1)].append(i)

    dups = {k: v for k, v in endpoints.items() if len(v) > 1}
    if dups:
        print(f'[{mod}]  ENDPOINT COLLISIONS: {dups}')
    else:
        print(f'[{mod}]  OK')

print()
print("=" * 70)
print("AUDIT: Menu routes vs actual endpoints")
print("=" * 70)

import json
menu_file = os.path.join(blueprints_dir, '.agent', 'menu_structure.json')
with open(menu_file, 'r', encoding='utf-8') as f:
    menu = json.load(f)

# Collect all function names (endpoints) per module
all_endpoints = {}
for mod in modules:
    route_file = os.path.join(blueprints_dir, mod, 'routes.py')
    if not os.path.exists(route_file):
        continue
    with open(route_file, 'r', encoding='utf-8') as f:
        source = f.read()
    try:
        tree = ast.parse(source)
    except:
        continue
    all_endpoints[mod] = set()
    # Collect explicit endpoint= values
    for line in source.splitlines():
        m = re.search(r"endpoint=['\"]([^'\"]+)['\"]", line)
        if m:
            all_endpoints[mod].add(m.group(1))
    # Collect def names (implicit endpoints)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            all_endpoints[mod].add(node.name)

for cat_name, cat_data in menu['menu_tree'].items():
    for module in cat_data.get('modules', []):
        route = module.get('route', '')
        if '.' not in route:
            continue
        bp, ep = route.split('.', 1)
        if bp not in all_endpoints:
            print(f'  MISSING blueprint [{bp}] for menu route: {route}')
        elif ep not in all_endpoints[bp]:
            print(f'  BROKEN menu route: {route} -> endpoint [{ep}] NOT FOUND in {bp}/routes.py')
        else:
            pass  # OK, silent

print("  (No output above = all menu routes resolve correctly)")
print("Done.")
