"""
Análisis y Sincronización de Permisos
Compara permisos del menú con los de la base de datos.
"""

import json
from pathlib import Path
from database import get_db_cursor

def main():
    # 1. Cargar permisos del menú
    menu_file = Path(__file__).parent / '.agent' / 'menu_structure.json'
    with open(menu_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    menu_perms = {}
    for category_name, category_data in data['menu_tree'].items():
        for module in category_data.get('modules', []):
            perm = module.get('permission')
            if perm and perm != 'N/A':
                menu_perms[perm] = {
                    'category': category_name,
                    'module': module['name']
                }
    
    # 2. Cargar permisos de la BD
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT code, category FROM sys_permissions WHERE enterprise_id = 0")
        db_perms = {row['code']: row['category'] for row in cursor.fetchall()}
    
    # 3. Comparar
    menu_only = set(menu_perms.keys()) - set(db_perms.keys())
    db_only = set(db_perms.keys()) - set(menu_perms.keys())
    common = set(menu_perms.keys()) & set(db_perms.keys())
    
    print("=" * 80)
    print("ANALISIS DE PERMISOS: MENU vs BASE DE DATOS")
    print("=" * 80)
    print(f"\nPermisos en MENU: {len(menu_perms)}")
    print(f"Permisos en BD: {len(db_perms)}")
    print(f"Comunes: {len(common)}")
    
    if menu_only:
        print(f"\n[!] FALTAN en BD ({len(menu_only)} permisos):")
        for perm in sorted(menu_only):
            info = menu_perms[perm]
            print(f"  - {perm:<35} [{info['category']}] {info['module']}")
    else:
        print("\n[OK] Todos los permisos del menu estan en la BD")
    
    if db_only:
        print(f"\n[i] En BD pero NO en menu ({len(db_only)} permisos):")
        for perm in sorted(db_only)[:10]:  # Mostrar solo primeros 10
            print(f"  - {perm}")
        if len(db_only) > 10:
            print(f"  ... y {len(db_only) - 10} mas")
    
    # 4. Ofrecer sincronización
    if menu_only:
        print("\n" + "=" * 80)
        respuesta = input("¿Desea registrar los permisos faltantes? (s/n): ")
        if respuesta.lower() == 's':
            registrar_permisos(menu_only, menu_perms)
    
    print("\n" + "=" * 80)

def registrar_permisos(permisos_faltantes, menu_perms):
    """Registra los permisos faltantes en la BD"""
    with get_db_cursor() as cursor:
        for perm in sorted(permisos_faltantes):
            info = menu_perms[perm]
            desc = info['module']
            cat = info['category']
            
            try:
                cursor.execute("""
                    INSERT INTO sys_permissions (enterprise_id, code, description, category)
                    VALUES (0, %s, %s, %s)
                """, (perm, desc, cat))
                print(f"  [+] Registrado: {perm}")
            except Exception as e:
                print(f"  [ERROR] {perm}: {e}")
    
    print("\n[OK] Sincronizacion completada!")

if __name__ == "__main__":
    main()
