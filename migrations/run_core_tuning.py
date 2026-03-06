import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_db_cursor

CORE_TABLES = [
    'stk_movimientos',
    'stk_articulos',
    'stk_saldos',
    'cmp_ordenes_compra',
    'cmp_detalles_orden',
    'erp_terceros'
]

def run_tuning():
    path = os.path.join(os.path.dirname(__file__), 'expert_tuning.sql')
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    cmds_to_run = []
    current_table = None
    for line in lines:
        if line.startswith('-- 📦 TABLA:'):
            current_table = line.split(':')[1].strip()
        elif line.startswith('CREATE INDEX'):
            if current_table in CORE_TABLES:
                cmds_to_run.append((current_table, line.strip()))

    with get_db_cursor() as c:
        for t, cmd in cmds_to_run:
            try:
                c.execute(cmd)
                print(f"✅ Executed on {t}: {cmd}")
            except Exception as e:
                # ignore duplicates
                if "Duplicate key name" not in str(e):
                    print(f"❌ Error on {t} -> {cmd} : {e}")
                else:
                    print(f"⏭️ Skip (already exists): {cmd}")

if __name__ == '__main__':
    run_tuning()
