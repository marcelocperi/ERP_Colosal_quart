import sys
import os

# Add root to path
sys.path.append(os.getcwd())

from database import get_db_cursor

def check_enterprise_leakage():
    try:
        # Core tables that should only have enterprise_id = 0 as templates
        tables = [
            'cont_plan_cuentas',
            'stk_mov_motivos',
            'stk_articulo_naturalezas'
        ]
        
        with get_db_cursor() as cursor:
            for t in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {t} WHERE enterprise_id != 0")
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"ALERTA: Tabla {t} tiene {count} registros con enterprise_id != 0")
                else:
                    print(f"Tabla {t}: OK (solo enterprise_id 0)")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_enterprise_leakage()
