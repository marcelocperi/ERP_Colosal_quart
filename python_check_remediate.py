import os
import re
import json
import sys
from datetime import datetime
from database import get_db_cursor, DB_CONFIG
import mariadb

# Configuración de Rutas Base
BASE_DIR = os.getcwd()
TEMPLATES_DIR = os.path.join(BASE_DIR, 'templates')
MENU_JSON = os.path.join(BASE_DIR, '.agent', 'menu_structure.json')
BASE_HTML = os.path.join(BASE_DIR, 'templates', 'base.html')

def log_incident_to_db(report_lines, status="ABIERTO"):
    """Inscribe el reporte de inconsistencias como un incidente en sys_transaction_logs."""
    if not report_lines: return
    try:
        summary = f"Reconciliación Global: {len(report_lines)} hallazgos encontrados."
        detail = "\n".join(report_lines)
        with get_db_cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'clob_data'")
            has_clob = bool(cursor.fetchone())
            col = 'clob_data' if has_clob else 'error_traceback'
            cursor.execute(f"""
                INSERT INTO sys_transaction_logs 
                (enterprise_id, user_id, module, status, severity, impact_category, 
                 failure_mode, error_message, {col}, incident_status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (1, 1, 'RECONCILIADOR', 'ERROR', 4, 'INTEGRIDAD', 
                  'CODE_DEBT', summary, detail, status, datetime.now()))
        print(f"✅ Incidente registrado en la base de datos.")
    except Exception as e:
        print(f"⚠️ No se pudo registrar el incidente: {e}")

def get_all_python_files():
    py_files = []
    for root, _, files in os.walk(BASE_DIR):
        if any(x in root for x in ['venv', '.git', '__pycache__', '.agent']): continue
        for file in files:
            if file.endswith('.py'): py_files.append(os.path.join(root, file))
    return py_files

def get_all_template_files():
    templates = {}
    for root, _, files in os.walk(BASE_DIR):
        if any(x in root for x in ['venv', '.git', '__pycache__', '.agent']): continue
        for file in files:
            if file.endswith('.html'):
                rel_path = os.path.relpath(os.path.join(root, file), BASE_DIR).replace('\\', '/')
                templates[rel_path] = os.path.join(root, file)
    return templates

def template_exists(template_name, bp_name, physical_templates):
    if not template_name: return True
    if f"templates/{template_name}" in physical_templates: return True
    if bp_name and f"{bp_name}/templates/{template_name}" in physical_templates: return True
    for pt in physical_templates:
        if pt.endswith(f"/{template_name}"): return True
    return False

def parse_codebase():
    routes = {}
    blueprints = {}
    py_files = get_all_python_files()
    
    bp_regex = re.compile(r"(\w+)\s*=\s*Blueprint\s*\(\s*['\"](\w+)['\"]")
    route_regex = re.compile(r"@(\w+)\.route\s*\(\s*['\"]([^'\"]+)['\"]")
    func_regex = re.compile(r"def\s+(\w+)\s*\(")
    render_regex = re.compile(r"render_template\s*\(\s*['\"]([^'\"]+)['\"]")
    import_regex = re.compile(r"from\s+(\w+)\.(\w+)\s+import")
    perm_regex = re.compile(r"@permission_required\s*\(\s*['\"]([^'\"]+)['\"]")
    
    found_permissions = set()

    # 1. Blueprints
    for py_file in py_files:
        try:
            with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                for match in bp_regex.finditer(content):
                    blueprints[match.group(1)] = match.group(2)
        except: pass

    # 2. Rutas e Imports
    broken_imports = []
    for py_file in py_files:
        try:
            with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                lines = content.splitlines()
                rel_folder = os.path.basename(os.path.dirname(py_file))
                
                for i, line in enumerate(lines):
                    # Chequear imports locales rotos
                    imp_match = import_regex.search(line)
                    if imp_match:
                        mod_base = imp_match.group(1)
                        mod_file = imp_match.group(2)
                        # Solo chequear si el mod_base es una carpeta de este proyecto
                        if os.path.isdir(os.path.join(BASE_DIR, mod_base)):
                            target_path = os.path.join(BASE_DIR, mod_base, f"{mod_file}.py")
                            if not os.path.exists(target_path):
                                broken_imports.append(f"ARCHIVO '{os.path.relpath(py_file, BASE_DIR)}' intenta importar de '{mod_base}.{mod_file}' que NO EXISTE.")

                    r_match = route_regex.search(line)
                    if r_match:
                        bp_var = r_match.group(1)
                        actual_bp = blueprints.get(bp_var, bp_var)
                        if bp_var in ['bp', 'bp_var'] and rel_folder not in ['', '.', 'multiMCP']:
                            actual_bp = rel_folder

                        func_name, template_name = None, None
                        for j in range(i+1, min(i+10, len(lines))):
                            f_match = func_regex.search(lines[j])
                            if f_match:
                                func_name = f_match.group(1)
                                for k in range(j+1, min(j+50, len(lines))):
                                    rd_match = render_regex.search(lines[k])
                                    if rd_match:
                                        template_name = rd_match.group(1)
                                        break
                                break
                        if func_name:
                            routes[f"{actual_bp}.{func_name}"] = {
                                'template': template_name, 'bp_name': actual_bp, 'file': os.path.relpath(py_file, BASE_DIR)
                            }
                    
                    # Chequear decoradores de permisos
                    for p_match in perm_regex.finditer(content):
                        found_permissions.add(p_match.group(1))
        except: pass
    return routes, blueprints, broken_imports, found_permissions

def create_missing_python_route(route_name, blueprints):
    if '.' not in route_name: return None
    bp_name, func_name = route_name.split('.')
    bp_var = next((k for k, v in blueprints.items() if v == bp_name), f"{bp_name}_bp")
    target_file = os.path.join(BASE_DIR, bp_name, "routes.py")
    if not os.path.exists(target_file):
        target_file = os.path.join(BASE_DIR, f"{bp_name}_routes.py")
    os.makedirs(os.path.dirname(target_file), exist_ok=True)
    is_new = not os.path.exists(target_file)
    with open(target_file, 'a', encoding='utf-8') as f:
        if is_new:
            f.write("from flask import Blueprint, render_template\n")
            f.write("from core.decorators import login_required\n\n")
            f.write(f"{bp_var} = Blueprint('{bp_name}', __name__)\n")
        f.write(f"\n\n@{bp_var}.route('/{bp_name}/{func_name.replace('_', '-')}')\n@login_required\ndef {func_name}():\n    return await render_template('{bp_name}/{func_name}.html')\n")
    return target_file

def reconcile():
    print("🔍 Iniciando Reconciliación Global v5 (Con Chequeo de Imports)...")
    if not os.path.exists(MENU_JSON): return
    
    python_routes, all_blueprints, broken_imports, found_permissions = parse_codebase()
    physical_templates = get_all_template_files()
    with open(MENU_JSON, 'r', encoding='utf-8') as f: menu_data = json.load(f)

    # 0. Agregar reportes de imports rotos
    report = []
    actions = []
    for bi in broken_imports: report.append(f"[IMPORT ERROR] {bi}")

    # --- NUEVO: AUDITORÍA DE PERMISOS (CISA/SOX) ---
    print("🔐 Auditando Permisos y Segregación de Funciones (SoD)...")
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Verificar existencia de permisos en DB
            cursor.execute("SELECT code FROM sys_permissions WHERE enterprise_id = 0")
            db_perms = {r['code'] for r in cursor.fetchall()}
            
            for p in found_permissions:
                if p not in db_perms:
                    report.append(f"[PERMISO FALTANTE] El permiso '{p}' está en el código pero no en la base de datos.")
                    # REMEDIACIÓN: Insertar permiso
                    cat = p.split('.')[0].upper() if '.' in p else 'SISTEMA'
                    cursor.execute("""
                        INSERT INTO sys_permissions (enterprise_id, code, description, category, user_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (0, p, f"Autogenerado: Acceso a {p}", cat, 1))
                    actions.append(f"FIX: Creado permiso '{p}' en DB.")

            # 2. Chequeo de SoD (Segregation of Duties)
            # Definir pares de conflicto (CISA Best Practices)
            SOD_CONFLICTS = [
                ('compras.admin', 'ventas.admin'),
                ('contabilidad.admin', 'fondos.admin'),
                ('cost_accounting', 'view_precios'), # Auditoría vs Operación
            ]
            
            cursor.execute("""
                SELECT r.name as role_name, p.code as perm_code, rp.role_id
                FROM sys_role_permissions rp
                JOIN sys_roles r ON rp.role_id = r.id AND rp.enterprise_id = r.enterprise_id
                JOIN sys_permissions p ON rp.permission_id = p.id AND rp.enterprise_id = p.enterprise_id
                WHERE rp.enterprise_id = 0
            """)
            role_perms = cursor.fetchall()
            
            # Agrupar por rol
            roles_map = {}
            for rp in role_perms:
                rid = rp['role_id']
                if rid not in roles_map: roles_map[rid] = {'name': rp['role_name'], 'perms': set()}
                roles_map[rid]['perms'].add(rp['perm_code'])
            
            for rid, rdata in roles_map.items():
                # EXCEPTIONS: adminSys allowed to have conflicts during dev
                if rdata['name'] == 'adminSys': continue 

                for c1, c2 in SOD_CONFLICTS:
                    if c1 in rdata['perms'] and c2 in rdata['perms']:
                        report.append(f"[SoD CONFLICT] El rol '{rdata['name']}' (ID {rid}) tiene permisos conflictivos: {c1} y {c2}.")

    except Exception as e:
        report.append(f"[DB ERROR] Falló auditoría de permisos: {e}")

    # --- FIN AUDITORÍA PERMISOS ---

    menu_routes = []
    def walk_menu(node):
        if isinstance(node, dict):
            if 'route' in node: menu_routes.append(node['route'])
            for v in node.values(): walk_menu(v)
        elif isinstance(node, list):
            for i in node: walk_menu(i)
    walk_menu(menu_data['menu_tree'])
    
    # ... (Resto del código de integridad) ...
    # 1. Alineación de Blueprints
    bp_corrections = {}
    for mr in menu_routes:
        if mr not in python_routes and '.' in mr:
            m_bp, m_fn = mr.split('.')
            for pr in python_routes:
                p_bp, p_fn = pr.split('.')
                if m_fn == p_fn: bp_corrections[mr] = pr; break

    def fix_menu_node(node):
        changed = False
        if isinstance(node, dict):
            if 'route' in node and node['route'] in bp_corrections:
                node['route'] = bp_corrections[node['route']]; changed = True
            for v in node.values():
                if fix_menu_node(v): changed = True
        elif isinstance(node, list):
            seen = set(); new_list = []
            for item in node:
                if isinstance(item, dict) and 'route' in item:
                    if item['route'] not in seen:
                        new_list.append(item); seen.add(item['route'])
                        if fix_menu_node(item): changed = True
                    else: changed = True
                else:
                    new_list.append(item)
                    if fix_menu_node(item): changed = True
            if len(new_list) != len(node): node[:] = new_list; changed = True
        return changed

    if fix_menu_node(menu_data['menu_tree']):
        with open(MENU_JSON, 'w', encoding='utf-8') as f:
            json.dump(menu_data, f, indent=4, ensure_ascii=False)
        actions.append("✅ Alineado menu_structure.json")

    # 2. Verificar Integridad
    for r, info in python_routes.items():
        if info['template'] and not template_exists(info['template'], info['bp_name'], physical_templates):
            report.append(f"[ERROR] Ruta '{r}' requiere '{info['template']}' pero no existe.")
            tpl_path = os.path.join(TEMPLATES_DIR, info['template'].replace('/', os.sep)) if '/' in info['template'] else os.path.join(BASE_DIR, info['bp_name'], 'templates', info['template'])
            os.makedirs(os.path.dirname(tpl_path), exist_ok=True)
            if not os.path.exists(tpl_path):
                with open(tpl_path, 'w', encoding='utf-8') as tf:
                    tf.write(f"{{% extends 'base.html' %}}\n{{% block content %}}\n<div class='container mt-4'><h4>Módulo Autogenerado: {r}</h4></div>\n{{% endblock %}}")
                actions.append(f"FIX: Creado template '{info['template']}'")

    for mr in menu_routes:
        if mr not in python_routes and '.' in mr and not mr.startswith('static'):
             report.append(f"[MENU ERROR] Ruta '{mr}' en menú pero no en Python.")
             if create_missing_python_route(mr, all_blueprints):
                 actions.append(f"FIX: Creado handler Python para '{mr}'")

    if report:
        log_incident_to_db(report + [f"ACCIONES: {a}" for a in actions])
        print(f"Finalizado con {len(report)} hallazgos. Ver incidentes.")
    else: print("✅ Todo en orden.")

if __name__ == "__main__":
    reconcile()
