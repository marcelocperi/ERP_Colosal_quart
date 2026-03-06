"""
Generador de visualización del árbol de menú del sistema ERP.
Permite visualizar la estructura jerárquica de navegación basada en operaciones de negocio.
"""

import json
from pathlib import Path

def generate_tree_visualization():
    """Genera una visualización ASCII del árbol de menú"""
    
    # Cargar estructura
    menu_file = Path(__file__).parent / '.agent' / 'menu_structure.json'
    with open(menu_file, 'r', encoding='utf-8') as f:
        menu_data = json.load(f)
    
    tree = menu_data['menu_tree']
    
    print("=" * 80)
    print("ESTRUCTURA JERARQUICA DEL SISTEMA ERP")
    print("Organización por Operaciones de Negocio")
    print("=" * 80)
    print()
    
    for category_name, category_data in tree.items():
        icon = category_data['icon'].split()[-1]  # Último elemento es el icono
        color = category_data['color']
        desc = category_data['description']
        
        print(f"[{category_name}]")
        print(f"  Descripción: {desc}")
        print(f"  Icono: {icon} | Color: {color}")
        print(f"  Módulos:")
        
        for i, module in enumerate(category_data['modules']):
            is_last = (i == len(category_data['modules']) - 1)
            prefix = "└─" if is_last else "├─"
            
            name = module['name']
            route = module.get('route', 'N/A')
            perm = module.get('permission', 'N/A')
            
            print(f"  {prefix} {name}")
            print(f"     Route: {route}")
            print(f"     Permission: {perm}")
        
        print()
    
    print("=" * 80)
    print(f"Total de categorías: {len(tree)}")
    total_modules = sum(len(cat['modules']) for cat in tree.values())
    print(f"Total de módulos: {total_modules}")
    print("=" * 80)

def generate_mermaid_diagram():
    """Genera un diagrama Mermaid para visualización web"""
    
    menu_file = Path(__file__).parent / '.agent' / 'menu_structure.json'
    with open(menu_file, 'r', encoding='utf-8') as f:
        menu_data = json.load(f)
    
    tree = menu_data['menu_tree']
    
    print("\n\n--- DIAGRAMA MERMAID ---")
    print("```mermaid")
    print("graph TD")
    print("    ROOT[Sistema ERP] --> COMPRA[💰 COMPRA]")
    print("    ROOT --> VENTA[💵 VENTA]")
    print("    ROOT --> PAGO[💸 PAGO]")
    print("    ROOT --> COBRANZAS[💰 COBRANZAS]")
    print("    ROOT --> STOCK[📦 STOCK]")
    print("    ROOT --> CONFIG[⚙️ CONFIGURACION]")
    print("    ROOT --> AUDIT[📊 AUDITORIA]")
    print()
    
    category_map = {
        'COMPRA': 'COMPRA',
        'VENTA': 'VENTA',
        'PAGO': 'PAGO',
        'COBRANZAS': 'COBRANZAS',
        'STOCK': 'STOCK',
        'CONFIGURACION': 'CONFIG',
        'AUDITORIA': 'AUDIT'
    }
    
    for cat_name, cat_data in tree.items():
        cat_id = category_map[cat_name]
        for i, mod in enumerate(cat_data['modules']):
            mod_id = f"{cat_id}_M{i+1}"
            mod_name = mod['name'].replace(' ', '<br>')
            print(f"    {cat_id} --> {mod_id}[{mod_name}]")
    
    print("```")

if __name__ == "__main__":
    generate_tree_visualization()
    generate_mermaid_diagram()
