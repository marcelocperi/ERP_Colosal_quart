"""
Auditoría de Rutas vs Permisos
================================
Recorre todos los archivos routes.py del proyecto y compara cada
`permission_required(...)` encontrado con lo que existe en sys_permissions.

Salida:
  [OK]      - Permiso existe en BD
  [MISSING] - Permiso referenciado en ruta pero NO existe en sys_permissions
  [INFO]    - Ruta protegida solo por login_required (sin permission_required)
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from database import get_db_cursor

ROUTE_FILES = [
    'biblioteca/routes.py',
    'compras/routes.py',
    'contabilidad/routes.py',
    'core/routes.py',
    'fondos/routes.py',
    'stock/routes.py',
    'utilitarios/routes.py',
    'ventas/routes.py',
]

BASE = Path(__file__).parent

# Regex para capturar: @bp.route(...) y los decoradores que le siguen
ROUTE_RE   = re.compile(r"@\w+\.route\(['\"]([^'\"]+)['\"](?:.*methods.*?\[([^\]]*)\])?\)")
PERM_RE    = re.compile(r"@permission_required\(['\"]([^'\"]+)['\"]\)")
DEFN_RE    = re.compile(r"^def (\w+)\(")
LOGIN_RE   = re.compile(r"@login_required")


def scan_routes(filepath: str) -> list[dict]:
    """Extrae todas las rutas con sus permisos del archivo."""
    path = BASE / filepath
    if not path.exists():
        print(f"  ⚠️  Archivo no encontrado: {filepath}")
        return []

    results = []
    with open(path, encoding='utf-8', errors='replace') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Detectar inicio de un bloque de ruta
        if ROUTE_RE.search(line):
            route_match = ROUTE_RE.search(line)
            route_url   = route_match.group(1)
            methods_raw = route_match.group(2) or 'GET'
            methods = [m.strip().strip("'\"") for m in methods_raw.split(',')]

            # Leer decoradores y def hasta encontrar la función
            perm_code  = None
            has_login  = False
            func_name  = None
            j = i + 1
            while j < len(lines) and j < i + 12:
                dl = lines[j].strip()
                pm = PERM_RE.search(dl)
                if pm:
                    perm_code = pm.group(1)
                if LOGIN_RE.search(dl):
                    has_login = True
                dm = DEFN_RE.search(dl)
                if dm:
                    func_name = dm.group(1)
                    break
                j += 1

            results.append({
                'module': filepath.split('/')[0],
                'file':   filepath,
                'url':    route_url,
                'methods': methods,
                'func':  func_name or '?',
                'login': has_login,
                'perm':  perm_code,
            })
        i += 1

    return results


def audit_all():
    # 1. Cargar permisos existentes en BD
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT code, description, category FROM sys_permissions WHERE enterprise_id = 0")
        db_perms = {r['code']: r for r in cursor.fetchall()}

    print(f"\n{'='*80}")
    print(f"AUDITORÍA DE RUTAS vs sys_permissions  ({len(db_perms)} permisos en BD)")
    print(f"{'='*80}\n")

    all_routes = []
    for rf in ROUTE_FILES:
        all_routes.extend(scan_routes(rf))

    missing = []
    ok      = []
    public  = []
    login_only = []

    for r in all_routes:
        perm = r['perm']
        if perm is None and not r['login']:
            public.append(r)
        elif perm is None and r['login']:
            login_only.append(r)
        elif perm in db_perms:
            ok.append(r)
        else:
            missing.append(r)

    # --- Mostrar MISSING primero (los más importantes) ---
    if missing:
        print(f"{'━'*80}")
        print(f"  ❌ FALTANTES EN sys_permissions ({len(missing)} rutas)")
        print(f"{'━'*80}")
        prev_mod = None
        for r in sorted(missing, key=lambda x: (x['module'], x['url'])):
            if r['module'] != prev_mod:
                print(f"\n  📁 {r['module'].upper()}")
                prev_mod = r['module']
            print(f"     [MISSING] {', '.join(r['methods']):<8} {r['url']:<45} → perm: '{r['perm']}'  fn: {r['func']}")
    else:
        print("  ✅ Todos los permisos referenciados existen en sys_permissions.\n")

    # --- Mostrar OK ---
    print(f"\n{'━'*80}")
    print(f"  ✅ PERMISOS CONFIRMADOS ({len(ok)} rutas)")
    print(f"{'━'*80}")
    prev_mod = None
    for r in sorted(ok, key=lambda x: (x['module'], x['url'])):
        if r['module'] != prev_mod:
            print(f"\n  📁 {r['module'].upper()}")
            prev_mod = r['module']
        cat = db_perms[r['perm']]['category'] or '?'
        print(f"     [OK]      {', '.join(r['methods']):<8} {r['url']:<45} perm: '{r['perm']}'  [{cat}]")

    # --- Login only ---
    print(f"\n{'━'*80}")
    print(f"  🔐 SOLO LOGIN_REQUIRED (sin permission_required) ({len(login_only)} rutas)")
    print(f"{'━'*80}")
    prev_mod = None
    for r in sorted(login_only, key=lambda x: (x['module'], x['url'])):
        if r['module'] != prev_mod:
            print(f"\n  📁 {r['module'].upper()}")
            prev_mod = r['module']
        print(f"     [LOGIN]   {', '.join(r['methods']):<8} {r['url']:<45} fn: {r['func']}")

    # --- Públicas ---
    print(f"\n{'━'*80}")
    print(f"  🌐 RUTAS PÚBLICAS (sin autenticación) ({len(public)} rutas)")
    print(f"{'━'*80}")
    for r in sorted(public, key=lambda x: x['url']):
        print(f"     [PUBLIC]  {', '.join(r['methods']):<8} {r['url']:<45} fn: {r['func']}")

    print(f"\n{'='*80}")
    print(f"  RESUMEN TOTAL:  {len(all_routes)} rutas")
    print(f"  ❌ Missing perms : {len(missing)}")
    print(f"  ✅ OK            : {len(ok)}")
    print(f"  🔐 Solo login    : {len(login_only)}")
    print(f"  🌐 Públicas      : {len(public)}")
    print(f"{'='*80}\n")

    return missing, ok, login_only, public, db_perms


if __name__ == "__main__":
    audit_all()
