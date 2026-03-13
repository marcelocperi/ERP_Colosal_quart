import json
import os
import re
from pathlib import Path

def audit_routes():
    # Load menu structure
    menu_file = Path(r'C:\Users\marce\Documents\GitHub\quart\.agent\menu_structure.json')
    if not menu_file.exists():
        print("Error: menu_structure.json not found")
        return
        
    with open(menu_file, 'r', encoding='utf-8') as f:
        menu_data = json.load(f)
        
    menu_routes = []
    for category_name, category_data in menu_data.get('menu_tree', {}).items():
        for module in category_data.get('modules', []):
            route = module.get('route')
            if route:
                menu_routes.append((route, module.get('name'), category_name))

    menu_routes_set = {r[0] for r in menu_routes}
    
    # Scan for backend routes
    backend_routes = []
    project_root = Path(r'C:\Users\marce\Documents\GitHub\quart')
    route_regex = re.compile(r'@([a-zA-Z0-9_]+)_bp\.route\(([\'"])([^\'"]+)\2')
    
    for py_file in project_root.rglob('*.py'):
        # Ignore venv and similar folders
        if 'venv' in py_file.parts or '.git' in py_file.parts or '__pycache__' in py_file.parts:
            continue
            
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # We need to find the function name associated with the route.
                # Since Blueprints are registered like ventaps_bp, the route name implies the blueprint.
                # Example: @stock_bp.route('/articulos') -> endpoints: stock.articulos
                # Finding def names
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if '@' in line and '_bp.route' in line:
                        match = re.search(r'@([a-zA-Z0-9_]+)_bp\.route', line)
                        if match:
                            bp_name = match.group(1)
                            # Look ahead for def
                            fn_name = None
                            for j in range(i+1, min(i+10, len(lines))):
                                fn_match = re.search(r'def\s+([a-zA-Z0-9_]+)\(', lines[j])
                                if fn_match:
                                    fn_name = fn_match.group(1)
                                    break
                            
                            if fn_name:
                                full_endpoint = f"{bp_name}.{fn_name}"
                                # Filter out aliases or API ones depending on rules, but let's record all
                                backend_routes.append({
                                    'endpoint': full_endpoint,
                                    'blueprint': bp_name,
                                    'function': fn_name,
                                    'file': str(py_file.relative_to(project_root))
                                })
        except Exception as e:
            pass
            
    backend_endpoints_set = {r['endpoint'] for r in backend_routes}
    
    print("=== AUDITORÍA DE RUTAS (FASE 1.1) ===")
    print(f"Total rutas en Menú: {len(menu_routes_set)}")
    print(f"Total endpoints detectados en código (ignorando APIs manuales y utilerías no-HTML si las hay): {len(backend_endpoints_set)}\n")
    
    print("--- 🚨 ENDPOINTS EN CÓDIGO PERO NO EN EL MENÚ (Pantallas Desaparecidas) ---")
    missing_in_menu = backend_endpoints_set - menu_routes_set
    for ep in sorted(missing_in_menu):
        # Filtrar rutas que claramente son APIs (contienen api_ o post_) o que necesitan IDs
        # porque no van en el menú principal.
        if ep.startswith('core.') or '.api_' in ep or '.post_' in ep or 'guardar' in ep or 'eliminar' in ep or 'editar' in ep:
            continue
        route_info = next((r for r in backend_routes if r['endpoint'] == ep), None)
        file_hint = route_info['file'] if route_info else "Desconocido"
        # Mostramos lo que parece ser una pantalla
        if 'api' not in ep and 'post' not in ep:
             print(f"- {ep}  (Archivo: {file_hint})")
             
    print("\n--- 🚨 RUTAS EN EL MENÚ PERO QUE NO EXISTEN EN EL CÓDIGO (Rutas Rotas) ---")
    missing_in_backend = menu_routes_set - backend_endpoints_set
    for m in sorted(menu_routes):
        if m[0] in missing_in_backend:
            print(f"- {m[0]} (Visible como '{m[1]}' en '{m[2]}')")

if __name__ == '__main__':
    audit_routes()
