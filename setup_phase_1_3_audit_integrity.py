import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from multiMCP.database import get_db_cursor

def upgrade_audit_fields():
    print("Reforzando integridad de Trazabilidad Cíclica...")
    with get_db_cursor() as cursor:
        # Agregar fecha_efectiva si no existe
        try:
            cursor.execute("ALTER TABLE stk_series_trazabilidad ADD COLUMN fecha_efectiva DATE DEFAULT NULL AFTER fecha")
            print("  Columna fecha_efectiva agregada.")
        except:
            print("  (fecha_efectiva ya existe)")

        # Asegurar que user_id sea NOT NULL
        try:
            cursor.execute("ALTER TABLE stk_series_trazabilidad MODIFY COLUMN user_id INT NOT NULL")
            print("  Restricción user_id NOT NULL aplicada.")
        except:
            pass

    print("Integridad de campos completada.")

if __name__ == "__main__":
    upgrade_audit_fields()
