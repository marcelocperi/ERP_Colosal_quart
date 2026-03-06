"""
Seed: Registro de Módulos FMECA en el Maestro de Perfiles
==========================================================
Inscribe los nuevos módulos del framework de Riesgo/Auditoría en sys_permissions,
agrupados por categoría según la estructura existente del menú.

Módulos nuevos:
  AUDITORIA:
    - Dashboard de Riesgos FMECA     (sysadmin)
    - Consulta de Errores            (admin_users)

  SISTEMA:
    - Riesgos Global (cross-tenant)  (sysadmin)

  SERVICIOS (internos, sin menú):
    - Risk Mitigation Service        (sysadmin)
    - Reglas de Mitigación           (sysadmin)
    - Historial de Mitigaciones      (sysadmin)
"""

from database import get_db_cursor

# ---------------------------------------------------------------------------
# Nuevos permisos granulares a registrar si no existen
# (code, description_detallada, category)
# ---------------------------------------------------------------------------
NEW_PERMISSIONS = [
    (
        'view_risk_dashboard',
        'Dashboard de Riesgos FMECA – Heatmap RPN, Failure Modes y Trend de errores por módulo.',
        'AUDITORIA'
    ),
    (
        'view_error_log',
        'Consulta de Errores del Sistema – Vista dual: Perfil Negocio (lenguaje amigable + datos intervinientes) y Perfil Experto (CLOB del traceback completo).',
        'AUDITORIA'
    ),
    (
        'manage_mitigation_rules',
        'Gestión de Reglas de Mitigación Activa – Alta, baja y modificación de reglas FMECA que disparan alertas o bloqueos automáticos.',
        'SISTEMA'
    ),
    (
        'view_mitigation_history',
        'Historial de Mitigaciones Activas – Visualización del log de acciones de respuesta automática tomadas por el sistema.',
        'AUDITORIA'
    ),
]

# ---------------------------------------------------------------------------
# Módulos registrados (para documentación del módulo, aunque ya usen
# permisos existentes como sysadmin / admin_users).
# ---------------------------------------------------------------------------
MODULE_REGISTRY = [
    # route_name                       | label                               | permission_code     | category
    ('core.admin_risk_dashboard',      'Dashboard de Riesgos FMECA',        'view_risk_dashboard',  'AUDITORIA'),
    ('core.error_log',                 'Consulta de Errores (Listado)',      'view_error_log',        'AUDITORIA'),
    ('core.error_log_detail',          'Consulta de Errores (Detalle+CLOB)', 'view_error_log',        'AUDITORIA'),
    ('core.admin_risk_dashboard',      'Riesgos Global Cross-Tenant',        'view_risk_dashboard',  'SISTEMA'),
]


def seed_fmeca_permissions(enterprise_id: int = 0):
    """Registra los nuevos permisos granulares en sys_permissions."""
    print("=" * 70)
    print(f"SEED DE MÓDULOS FMECA EN MAESTRO DE PERFILES (enterprise_id={enterprise_id})")
    print("=" * 70)

    added, skipped = 0, 0

    with get_db_cursor(dictionary=True) as cursor:
        for code, description, category in NEW_PERMISSIONS:
            # Verificar si ya existe
            cursor.execute(
                "SELECT id FROM sys_permissions WHERE code = %s AND enterprise_id = %s",
                (code, enterprise_id)
            )
            existing = cursor.fetchone()

            if existing:
                # Actualizar descripción si cambió
                cursor.execute(
                    "UPDATE sys_permissions SET description=%s, category=%s WHERE id=%s",
                    (description, category, existing['id'])
                )
                print(f"  [≈] ACTUALIZADO  | {category:15} | {code}")
                skipped += 1
            else:
                cursor.execute(
                    """INSERT INTO sys_permissions (enterprise_id, code, description, category)
                       VALUES (%s, %s, %s, %s)""",
                    (enterprise_id, code, description, category)
                )
                print(f"  [+] INSERTADO    | {category:15} | {code}")
                added += 1

    print(f"\n✅ Insertados: {added}  |  Actualizados: {skipped}")


def update_menu_with_new_routes():
    """
    Actualiza el menu_structure.json para que los nuevos módulos
    usen los permisos granulares recién creados (en vez de 'sysadmin'/'admin_users' genérico).
    """
    import json
    from pathlib import Path

    menu_file = Path(__file__).parent / '.agent' / 'menu_structure.json'
    with open(menu_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Mapa de rutas → nuevo permiso granular
    route_to_perm = {
        'core.admin_risk_dashboard': 'view_risk_dashboard',
        'core.error_log':            'view_error_log',
    }

    changed = 0
    for cat_name, cat_data in data['menu_tree'].items():
        for module in cat_data.get('modules', []):
            route = module.get('route', '')
            if route in route_to_perm:
                old = module.get('permission')
                new = route_to_perm[route]
                if old != new:
                    module['permission'] = new
                    print(f"  [menu] {route}: '{old}' → '{new}'")
                    changed += 1

    if changed:
        with open(menu_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"\n✅ Menu actualizado: {changed} rutas con permisos granulares.")
    else:
        print("\n✓ El menú ya tiene los permisos correctos.")


def print_module_registry():
    """Imprime el registro de módulos para documentación."""
    print("\n" + "=" * 70)
    print("REGISTRO DE MÓDULOS FMECA")
    print("=" * 70)
    print(f"{'Ruta Flask':<40} {'Permiso':<25} {'Categoría'}")
    print("-" * 70)
    for route, label, perm, cat in MODULE_REGISTRY:
        print(f"  {route:<38} {perm:<25} {cat}")
    print("=" * 70)


if __name__ == "__main__":
    # 1. Registrar permisos granulares para enterprise_id=0 (global)
    seed_fmeca_permissions(enterprise_id=0)

    # 2. Actualizar el menú para usar permisos granulares
    print("\n" + "=" * 70)
    print("ACTUALIZANDO menu_structure.json CON PERMISOS GRANULARES")
    print("=" * 70)
    update_menu_with_new_routes()

    # 3. Re-sincronizar con BD para que queden los nuevos en sys_permissions
    print("\n" + "=" * 70)
    print("RE-SINCRONIZACIÓN FINAL CON BD")
    print("=" * 70)
    from sync_menu_permissions import sync_permissions_to_database
    sync_permissions_to_database(enterprise_id=0)

    # 4. Imprimir registro
    print_module_registry()
