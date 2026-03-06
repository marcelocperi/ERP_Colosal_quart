"""
Sincronizador de Permisos desde menu_structure.json
Extrae todos los permisos únicos del menú y los registra en sys_permissions.
"""

import json
from pathlib import Path
from database import get_db_cursor

def extract_permissions_from_menu():
    """Extrae todos los permisos únicos del archivo de estructura del menú"""
    menu_file = Path(__file__).parent / '.agent' / 'menu_structure.json'
    
    with open(menu_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    permissions = set()
    category_map = {}
    
    for category_name, category_data in data['menu_tree'].items():
        for module in category_data.get('modules', []):
            perm = module.get('permission')
            if perm and perm != 'N/A':
                permissions.add(perm)
                category_map[perm] = category_name
    
    return permissions, category_map

def sync_permissions_to_database(enterprise_id=0):
    """
    Sincroniza los permisos del menú con la base de datos.
    
    Args:
        enterprise_id: ID de la empresa (0 para global/plantilla)
    """
    permissions, category_map = extract_permissions_from_menu()
    
    print("=" * 70)
    print(f"SINCRONIZACIÓN DE PERMISOS AL MAESTRO (enterprise_id={enterprise_id})")
    print("=" * 70)
    
    with get_db_cursor(dictionary=True) as cursor:
        # Primero, obtener permisos existentes
        cursor.execute("""
            SELECT code FROM sys_permissions WHERE enterprise_id = %s
        """, (enterprise_id,))
        existing = {row['code'] for row in cursor.fetchall()}
        
        print(f"\nPermisos existentes en BD: {len(existing)}")
        print(f"Permisos en menu_structure.json: {len(permissions)}")
        
        # Identificar nuevos permisos
        new_permissions = permissions - existing
        
        if new_permissions:
            print(f"\n✅ Nuevos permisos a registrar: {len(new_permissions)}")
            for perm in sorted(new_permissions):
                category = category_map.get(perm, 'General')
                
                # Generar descripción automática
                description = perm.replace('_', ' ').title()
                
                try:
                    cursor.execute("""
                        INSERT INTO sys_permissions (enterprise_id, code, description, category)
                        VALUES (%s, %s, %s, %s)
                    """, (enterprise_id, perm, description, category))
                    print(f"  [+] {perm} ({category})")
                except Exception as e:
                    print(f"  [ERROR] {perm}: {e}")
        else:
            print("\n✓ Todos los permisos ya están registrados. No hay nada que agregar.")
        
        # Mostrar permisos obsoletos (en BD pero no en menú)
        obsolete = existing - permissions
        if obsolete:
            print(f"\n⚠️  Permisos en BD pero NO en menú (posiblemente obsoletos): {len(obsolete)}")
            for perm in sorted(obsolete):
                print(f"  [-] {perm}")
    
    print("\n" + "=" * 70)
    print("SINCRONIZACIÓN COMPLETADA")
    print("=" * 70)

def generate_permissions_report():
    """Genera un reporte de todos los permisos por categoría"""
    permissions, category_map = extract_permissions_from_menu()
    
    # Agrupar por categoría
    by_category = {}
    for perm in permissions:
        cat = category_map.get(perm, 'General')
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(perm)
    
    # Generar reporte
    report = []
    report.append("# REPORTE DE PERMISOS DEL SISTEMA")
    report.append("=" * 70)
    report.append("")
    
    for category, perms in sorted(by_category.items()):
        report.append(f"\n## {category} ({len(perms)} permisos)")
        report.append("-" * 70)
        for perm in sorted(perms):
            report.append(f"  - {perm}")
    
    report.append(f"\n\n**TOTAL: {len(permissions)} permisos únicos**")
    
    report_text = "\n".join(report)
    
    # Guardar a archivo
    report_file = Path(__file__).parent / '.agent' / 'permissions_report.txt'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_text)
    
    print(report_text)
    print(f"\n📄 Reporte guardado en: {report_file}")

if __name__ == "__main__":
    print("\n1. Generando reporte de permisos...")
    generate_permissions_report()
    
    print("\n2. Sincronizando con base de datos (enterprise_id=0)...")
    sync_permissions_to_database(enterprise_id=0)
