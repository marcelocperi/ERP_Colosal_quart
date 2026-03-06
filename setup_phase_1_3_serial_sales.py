import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from multiMCP.database import get_db_cursor

def upgrade_series_for_sales():
    print("Fase 1.3 — Vinculando stk_series con comprobantes de venta...")
    with get_db_cursor() as cursor:
        # Link serial to the sales invoice where it was dispatched
        for col, definition in [
            ('comprobante_venta_id', 'INT DEFAULT NULL'),
            ('tercero_id',           'INT DEFAULT NULL COMMENT "Cliente al que se vendio"'),
            ('fecha_egreso',         'DATE DEFAULT NULL'),
            ('comprobante_nc_id',    'INT DEFAULT NULL COMMENT "NC de devolucion"'),
            ('fecha_devolucion',     'DATE DEFAULT NULL'),
        ]:
            try:
                cursor.execute(f"ALTER TABLE stk_series ADD COLUMN {col} {definition}")
                print(f"  Columna {col} agregada.")
            except Exception as e:
                print(f"  ({col} ya existe o error: {e})")

        # Index for quick customer lookups
        try:
            cursor.execute("CREATE INDEX idx_series_tercero ON stk_series (tercero_id)")
            cursor.execute("CREATE INDEX idx_series_comp ON stk_series (comprobante_venta_id)")
            print("  Índices creados.")
        except:
            pass

    print("Upgrade completado.")

if __name__ == "__main__":
    upgrade_series_for_sales()
