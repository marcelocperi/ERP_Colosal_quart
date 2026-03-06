"""
Menu Structure Loader - Context Processor para Flask
Carga la estructura jerárquica del menú desde el archivo JSON de configuración.
"""

import json
from pathlib import Path
from flask import g, url_for

def load_menu_structure():
    """Carga la estructura del menú desde el archivo JSON"""
    menu_file = Path(__file__).parent.parent / '.agent' / 'menu_structure.json'
    
    try:
        with open(menu_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data['menu_tree']
    except FileNotFoundError:
        # Fallback to empty structure
        return {}
    except json.JSONDecodeError:
        return {}

def inject_menu_structure():
    """Context processor para inyectar la estructura del menú en todos los templates"""
    return {
        'menu_structure': load_menu_structure()
    }

def has_permission(permission_name, user_permissions):
    """
    Verifica si el usuario tiene un permiso específico.
    
    Casos especiales:
    - permission_name == 'all': visible para CUALQUIER usuario logueado (wildcard de menú)
    - 'all' en user_permissions: el usuario es admin → ve todo
    - 'sysadmin' en user_permissions: superadmin → ve todo
    """
    if not user_permissions:
        return False
    
    # Marcador especial en el JSON: "permission": "all" → visible para todos
    if permission_name == 'all':
        return True
    
    # Sysadmin o admin local tiene todos los permisos
    if 'all' in user_permissions or 'sysadmin' in user_permissions:
        return True
    
    return permission_name in user_permissions

def filter_menu_by_permissions(menu_structure, user_permissions):
    """
    Filtra el menú según los permisos del usuario.
    
    Args:
        menu_structure: Estructura completa del menú
        user_permissions: Lista de permisos del usuario
    
    Returns:
        dict: Estructura del menú filtrada
    """
    filtered_menu = {}
    
    for category_name, category_data in menu_structure.items():
        # Procesar módulos dentro de la categoría y marcar accesos
        processed_modules = []
        for module in category_data.get('modules', []):
            module_copy = module.copy()
            module_copy['has_access'] = has_permission(module.get('permission', 'none'), user_permissions)
            # Pre-computar URL con manejo de error: si la ruta no existe (blueprint no registrado)
            # se asigna None y el template la omite en lugar de crashear
            try:
                module_copy['url'] = url_for(module['route'])
            except Exception:
                module_copy['url'] = None
            processed_modules.append(module_copy)
            
        filtered_menu[category_name] = {
            **category_data,
            'modules': processed_modules
        }
    
    return filtered_menu
