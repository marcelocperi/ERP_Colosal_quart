from database import get_db_cursor
import sys
sys.stdout.reconfigure(encoding='utf-8')

print("Consultando permisos actuales en la BD...\n")

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("""
        SELECT code, description, category, enterprise_id 
        FROM sys_permissions 
        WHERE enterprise_id = 0
        ORDER BY category, code
    """)
    
    permisos = cursor.fetchall()
    
    # Agrupar por categoría
    por_categoria = {}
    for p in permisos:
        cat = p['category'] or 'Sin Categoria'
        if cat not in por_categoria:
            por_categoria[cat] = []
        por_categoria[cat].append(p)
    
    print("=" * 70)
    print("PERMISOS REGISTRADOS EN SYS_PERMISSIONS (enterprise_id=0)")
    print("=" * 70)
    
    for categoria, perms in sorted(por_categoria.items()):
        print(f"\n{categoria} ({len(perms)} permisos)")
        print("-" * 70)
        for p in perms:
            print(f"  - {p['code']:<40} | {p['description']}")
    
    print(f"\n{'=' * 70}")
    print(f"TOTAL: {len(permisos)} permisos en la base de datos")
    print("=" * 70)
