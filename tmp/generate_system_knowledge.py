
import os
import json
import re

# Directorios de interés
PROJECT_ROOT = "."
RULES_DIR = os.path.join(PROJECT_ROOT, ".brain", "rules")
KNOWLEDGE_FILE = os.path.join(RULES_DIR, "system_logic_map.md")

# Módulos para escaneo de lógica
MODULES = ["core", "compras", "stock", "biblioteca", "ventas", "fondos", "services"]

def extract_routes_knowledge():
    print("Extracting Routes and Controllers Knowledge...")
    routes_info = []
    route_pattern = re.compile(r"@\w+\.route\(['\"]([^'\"]+)['\"]\s*,\s*methods=\[([^\]]+)\]", re.IGNORECASE)
    def_pattern = re.compile(r"def\s+(\w+)\s*\(", re.IGNORECASE)

    for module in MODULES:
        path = os.path.join(PROJECT_ROOT, module)
        if not os.path.exists(path): continue
        
        for root, _, files in os.walk(path):
            for file in files:
                if file.endswith("routes.py") or file.endswith("service.py"):
                    file_path = os.path.join(root, file)
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        lines = f.readlines()
                        current_route = None
                        for line in lines:
                            route_match = route_pattern.search(line)
                            if route_match:
                                current_route = {"url": route_match.group(1), "methods": route_match.group(2).replace("'", "").replace('"', ""), "module": module}
                            
                            def_match = def_pattern.search(line)
                            if def_match and current_route:
                                current_route["function"] = def_match.group(1)
                                routes_info.append(current_route)
                                current_route = None
    return routes_info

def extract_services_logic():
    print("Extracting Business Services Logic...")
    services_info = []
    class_pattern = re.compile(r"class\s+(\w+)", re.IGNORECASE)
    method_pattern = re.compile(r"def\s+(\w+)\s*\(self", re.IGNORECASE)
    classmethod_pattern = re.compile(r"@classmethod\s*\n\s*def\s+(\w+)", re.IGNORECASE)

    services_path = os.path.join(PROJECT_ROOT, "services")
    if os.path.exists(services_path):
        for file in os.listdir(services_path):
            if file.endswith(".py"):
                path = os.path.join(services_path, file)
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                    classes = class_pattern.findall(content)
                    methods = method_pattern.findall(content)
                    services_info.append({"file": file, "classes": classes, "methods": methods[:10]}) # Top 10 methods per file
    return services_info

def extract_menu_structure():
    print("Extracting Menu and UI Architecture...")
    menu_path = os.path.join(PROJECT_ROOT, ".agent", "menu_structure.json")
    if os.path.exists(menu_path):
        with open(menu_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def generate_knowledge_map():
    routes = extract_routes_knowledge()
    services = extract_services_logic()
    menu = extract_menu_structure()

    markdown = "# Mapa Lógico de Conocimiento - Colosal ERP\n\n"
    markdown += "Este documento sirve como arquitectura de referencia para el LLM Local.\n\n"

    markdown += "## 1. Arquitectura de Endpoints (Rutas)\n"
    markdown += "| Módulo | Ruta | Función | Métodos |\n"
    markdown += "| :--- | :--- | :--- | :--- |\n"
    for r in routes[:50]: # Cap to 50 for context size
        markdown += f"| {r['module']} | `{r['url']}` | `{r['function']}` | {r['methods']} |\n"

    markdown += "\n## 2. Servicios de Negocio (Core Logic)\n"
    for s in services:
        markdown += f"- **{s['file']}**: Clases `{', '.join(s['classes'])}`. Métodos clave: {', '.join(s['methods'])}\n"

    markdown += "\n## 3. Estructura de Navegación y Permisos\n"
    if menu and "menu" in menu:
        for cat in menu["menu"]:
            markdown += f"### Categoría: {cat.get('category', 'Sin Nombre')}\n"
            for item in cat.get("items", []):
                markdown += f"- {item.get('label')} -> Ruta: `{item.get('route')}` (Permiso: `{item.get('permission')}`)\n"

    if not os.path.exists(RULES_DIR):
        os.makedirs(RULES_DIR)

    with open(KNOWLEDGE_FILE, "w", encoding="utf-8") as f:
        f.write(markdown)
    
    print(f"✓ Conocimiento Lógico generado en {KNOWLEDGE_FILE}")

if __name__ == "__main__":
    generate_knowledge_map()
