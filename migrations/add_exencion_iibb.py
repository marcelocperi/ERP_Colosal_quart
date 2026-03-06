"""
Migración incremental: Agrega exencion_iibb a tax_reglas y carga las reglas
de exención CABA (Monotributista profesional exento de IIBB AGIP).

Ejecutar: python migrations/add_exencion_iibb.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor


def run():
    print("🚀 Migración: exencion_iibb en tax_reglas...")

    with get_db_cursor(dictionary=True) as cursor:

        # 1. Verificar si la columna ya existe
        cursor.execute("SHOW COLUMNS FROM tax_reglas LIKE 'exencion_iibb'")
        exists = cursor.fetchone()

        if not exists:
            print("  📌 Agregando columna exencion_iibb...")
            cursor.execute("""
                ALTER TABLE tax_reglas
                ADD COLUMN exencion_iibb VARCHAR(20) NOT NULL DEFAULT '*'
                COMMENT '* = cualquiera | EXENTO = solo si padron informa exento | NO_EXENTO = solo si NO es exento'
                AFTER condicion_iibb
            """)

            # Actualizar índices para incluir la nueva columna
            try:
                cursor.execute("ALTER TABLE tax_reglas DROP INDEX uq_regla")
            except Exception:
                pass
            try:
                cursor.execute("ALTER TABLE tax_reglas DROP INDEX idx_lookup")
            except Exception:
                pass

            cursor.execute("""
                ALTER TABLE tax_reglas
                ADD UNIQUE KEY uq_regla
                    (enterprise_id, operacion, tipo_responsable, condicion_iibb, exencion_iibb, impuesto_id)
            """)
            cursor.execute("""
                ALTER TABLE tax_reglas
                ADD INDEX idx_lookup
                    (enterprise_id, operacion, tipo_responsable, condicion_iibb, exencion_iibb)
            """)
            print("  ✅ Columna y índices creados.")
        else:
            print("  ℹ️  Columna exencion_iibb ya existe, saltando ALTER.")

        # 2. Obtener IDs de impuestos necesarios
        cursor.execute("SELECT id, codigo FROM tax_impuestos WHERE codigo IN ('OTROS_IMP','IIBB_AGIP','IIBB_ARBA')")
        imp_map = {r['codigo']: r['id'] for r in cursor.fetchall()}

        otros_id = imp_map.get('OTROS_IMP')
        agip_id  = imp_map.get('IIBB_AGIP')
        arba_id  = imp_map.get('IIBB_ARBA')

        if not all([otros_id, agip_id, arba_id]):
            print("  ❌ No se encontraron todos los impuestos necesarios. Ejecutar primero create_tax_engine_tables.py")
            return

        # 3. Insertar reglas de exención CABA
        # ─────────────────────────────────────────────────────────────────────
        # REGLA: MONOTRIBUTO + AGIP + EXENTO → sin IIBB (solo OTROS_IMP)
        # Profesional monotributista en CABA con exención impositiva informada por AGIP
        # ─────────────────────────────────────────────────────────────────────
        reglas_exencion = [
            # (operacion, tipo_resp, cond_iibb, exencion, imp_id, descripcion)
            ('COMPRAS', 'MONOTRIBUTO',   'AGIP',  'EXENTO',    otros_id,
             'Monotributo CABA exento: sin IIBB AGIP'),
            ('COMPRAS', 'MONOTRIBUTO',   'AGIP',  'NO_EXENTO', agip_id,
             'Monotributo CABA no exento: aplica IIBB AGIP'),
            ('COMPRAS', 'MONOTRIBUTO',   'AGIP',  'NO_EXENTO', otros_id,
             'Monotributo CABA no exento: otros impuestos'),
            # AMBOS: ARBA siempre aplica, AGIP solo si no exento
            ('COMPRAS', 'MONOTRIBUTO',   'AMBOS', 'EXENTO',    arba_id,
             'Monotributo AMBOS exento AGIP: ARBA aplica igual'),
            ('COMPRAS', 'MONOTRIBUTO',   'AMBOS', 'EXENTO',    otros_id,
             'Monotributo AMBOS exento AGIP: otros impuestos'),
            ('COMPRAS', 'MONOTRIBUTISTA','AGIP',  'EXENTO',    otros_id,
             'Monotributista CABA exento: sin IIBB AGIP'),
            ('COMPRAS', 'MONOTRIBUTISTA','AGIP',  'NO_EXENTO', agip_id,
             'Monotributista CABA no exento: aplica IIBB AGIP'),
            ('COMPRAS', 'MONOTRIBUTISTA','AGIP',  'NO_EXENTO', otros_id,
             'Monotributista CABA no exento: otros impuestos'),
        ]

        print(f"  📦 Insertando {len(reglas_exencion)} reglas de exención CABA...")
        inserted = 0
        for operacion, tipo, cond, exencion, imp_id, desc in reglas_exencion:
            cursor.execute("""
                INSERT IGNORE INTO tax_reglas
                    (enterprise_id, operacion, tipo_responsable, condicion_iibb,
                     exencion_iibb, impuesto_id, aplica)
                VALUES (0, %s, %s, %s, %s, %s, 1)
            """, (operacion, tipo, cond, exencion, imp_id))
            if cursor.rowcount > 0:
                inserted += 1
                print(f"    ✅ {desc}")

        print(f"\n  Total insertadas: {inserted}/{len(reglas_exencion)}")


    # 4. Crear Versión
    try:
        from services.tax_engine import TaxEngine
        engine = TaxEngine(enterprise_id=0)
        ver = engine.create_version("Migración: exencion_iibb + reglas CABA", user_id=1)
        print(f"  📌 Versión creada: {ver}")
    except Exception as e:
        print(f"  ⚠️  No se pudo crear versión: {e}")

    print("""
✅ Migración completada.

Lógica implementada:
  MONOTRIBUTO + AGIP + EXENTO   → Sin IIBB (profesional exento en CABA)
  MONOTRIBUTO + AGIP + NO_EXENTO → Con IIBB AGIP (presunción de que corresponde)
  MONOTRIBUTO + AMBOS + EXENTO  → ARBA aplica, AGIP no (exento solo en CABA)

El campo exencion_iibb del proveedor se obtiene del padrón AGIP.
Si no viene informado, el engine asume NO_EXENTO (presunción de que corresponde IIBB).
""")


if __name__ == '__main__':
    run()
