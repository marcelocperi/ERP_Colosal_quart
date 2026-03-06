"""
Script interactivo para ejecutar el seed de Plan de Cuentas MSAC v4.0.
Pregunta el enterprise_id y ejecuta el SQL sustituyendo la variable.
"""
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_cursor

def run_seed(enterprise_id: int):
    sql_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'migrations', 'seed_plan_cuentas_msac_v4.sql'
    )
    with open(sql_path, 'r', encoding='utf-8') as f:
        raw_sql = f.read()

    # Reemplazar la variable SQL con el valor real
    raw_sql = f"SET @ENT_ID = {enterprise_id};\n" + raw_sql

    statements = []
    for s in raw_sql.split(';'):
        s = s.strip()
        # Ignorar comentarios puros
        lines = [l for l in s.splitlines() if l.strip() and not l.strip().startswith('--')]
        clean = '\n'.join(lines).strip()
        if clean:
            statements.append(clean)

    with get_db_cursor() as cursor:
        ok = 0
        skip = 0
        for stmt in statements:
            try:
                cursor.execute(stmt)
                ok += 1
            except Exception as e:
                err = str(e)
                if 'Duplicate' in err or 'already exists' in err:
                    skip += 1
                else:
                    print(f"  [!] Error: {err[:120]}")
        print(f"\n  ✅ Seed completado: {ok} OK / {skip} ignorados (ya existían)")

if __name__ == '__main__':
    print("=" * 55)
    print("   SEED PLAN DE CUENTAS MSAC v4.0 — Colosal ERP")
    print("=" * 55)

    try:
        eid = int(input("Ingrese el ID de empresa (enterprise_id): ").strip())
    except ValueError:
        print("[!] ID inválido. Abortando.")
        sys.exit(1)

    confirm = input(f"\n¿Crear/actualizar cuentas para empresa ID={eid}? [s/N]: ").strip().lower()
    if confirm != 's':
        print("Operación cancelada.")
        sys.exit(0)

    run_seed(eid)
