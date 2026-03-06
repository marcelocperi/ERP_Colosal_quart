"""
Test rápido de las rutas principales del sistema
"""
from app import app

print("\n" + "="*70)
print("RUTAS REGISTRADAS EN EL SISTEMA")
print("="*70)

# Agrupar por módulo
routes_by_module = {
    'biblioteca': [],
    'stock': [],
    'compras': [],
    'ventas': [],
    'core': [],
    'otros': []
}

for rule in app.url_map.iter_rules():
    route_str = f"{sorted(rule.methods - {'HEAD', 'OPTIONS'})} {rule.rule}"
    
    if 'biblioteca' in rule.endpoint:
        routes_by_module['biblioteca'].append(route_str)
    elif 'stock' in rule.endpoint:
        routes_by_module['stock'].append(route_str)
    elif 'compras' in rule.endpoint:
        routes_by_module['compras'].append(route_str)
    elif 'ventas' in rule.endpoint:
        routes_by_module['ventas'].append(route_str)
    elif 'core' in rule.endpoint or 'ent_bp' in rule.endpoint:
        routes_by_module['core'].append(route_str)
    else:
        routes_by_module['otros'].append(route_str)

for module, routes in routes_by_module.items():
    if routes:
        print(f"\n{module.upper()} ({len(routes)} rutas):")
        for r in sorted(routes):
            print(f"  {r}")

print("\n" + "="*70)
print(f"TOTAL: {sum(len(r) for r in routes_by_module.values())} rutas registradas")
print("="*70)
