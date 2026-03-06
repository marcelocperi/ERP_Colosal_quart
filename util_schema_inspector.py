
import os, sys
sys.path.append(os.getcwd())
from database import get_db_cursor

TABLES = [
    'erp_terceros',
    'stk_articulos',
    'erp_comprobantes',
    'imp_despachos',
    'compras_ordenes',
    'fin_condiciones_pago',
    'fin_medios_pago',
]

with get_db_cursor(dictionary=True) as cursor:
    for table in TABLES:
        try:
            cursor.execute(f"DESCRIBE `{table}`")
            rows = cursor.fetchall()
            cols = [r['Field'] for r in rows]
            print(f"\n=== {table} ({len(cols)} cols) ===")
            print(", ".join(cols))
        except Exception as e:
            print(f"\n*** {table}: ERROR -> {e}")
