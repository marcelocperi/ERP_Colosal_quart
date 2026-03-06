"""
Poblar fin_bancos con datos iniciales:
  1. Bancos CBU desde la API del BCRA
  2. Billeteras CVU (listado semilla)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

from services.bcra_service import BCRAService

print("=" * 60)
print("PASO 1: Sincronizando bancos CBU desde API BCRA...")
print("=" * 60)
try:
    stats = BCRAService.sincronizar_desde_bcra(enterprise_id=0)
    print(f"  Insertados : {stats['insertados']}")
    print(f"  Actualizados: {stats['actualizados']}")
    print(f"  Cuentas creadas: {stats['cuentas']}")
    print(f"  Errores    : {stats['errores']}")
    print(f"  Total BCRA : {stats['total']}")
except Exception as e:
    print(f"  ERROR CBU: {e}")

print()
print("=" * 60)
print("PASO 2: Sincronizando billeteras virtuales CVU...")
print("=" * 60)
try:
    stats = BCRAService.sincronizar_billeteras(enterprise_id=0)
    print(f"  Insertadas : {stats['insertados']}")
    print(f"  Actualizadas: {stats['actualizados']}")
    print(f"  Cuentas creadas: {stats['cuentas']}")
    print(f"  Errores    : {stats['errores']}")
    print(f"  Total      : {stats['total']}")
except Exception as e:
    print(f"  ERROR CVU: {e}")

print()
print("=" * 60)
print("RESULTADO FINAL en fin_bancos:")
print("=" * 60)
from database import get_db_cursor
with get_db_cursor(dictionary=True) as c:
    c.execute("""
        SELECT b.tipo_entidad, b.nombre, b.codigo_cbu,
               c.codigo AS cuenta_codigo
        FROM fin_bancos b
        LEFT JOIN cont_plan_cuentas c ON b.cuenta_contable_id = c.id
        ORDER BY b.tipo_entidad, b.nombre
    """)
    rows = c.fetchall()
    cbu = [r for r in rows if r['tipo_entidad'] == 'CBU']
    cvu = [r for r in rows if r['tipo_entidad'] == 'CVU']

    print(f"\n  Bancos CBU ({len(cbu)}):")
    for r in cbu[:10]:
        print(f"    {r['codigo_cbu']}  {r['nombre']:<40} -> {r['cuenta_codigo'] or 'sin cuenta'}")
    if len(cbu) > 10:
        print(f"    ... y {len(cbu)-10} más")

    print(f"\n  Billeteras CVU ({len(cvu)}):")
    for r in cvu:
        print(f"    {r['codigo_cbu']}  {r['nombre']:<40} -> {r['cuenta_codigo'] or 'sin cuenta'}")

print(f"\n  TOTAL: {len(rows)} entidades")
