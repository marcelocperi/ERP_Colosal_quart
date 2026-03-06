import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from multiMCP.database import get_db_cursor

def setup_phase_1_4_masterbox():
    print("Fase 1.4 — Infraestructura para Master Boxes y Agregación...")
    with get_db_cursor() as cursor:
        # 1. Relación Padre-Hijo en Series
        try:
            cursor.execute("ALTER TABLE stk_series ADD COLUMN parent_id INT DEFAULT NULL COMMENT 'ID del Master Box que la contiene'")
            cursor.execute("ALTER TABLE stk_series ADD COLUMN es_contenedor TINYINT(1) DEFAULT 0 COMMENT '1 si es una caja/pallet'")
            print("  Columnas de jerarquía agregadas a stk_series.")
        except:
            print("  (Jerarquía ya existe en stk_series)")

        # 2. Flag de Masterbox en códigos de alias
        try:
            cursor.execute("ALTER TABLE stk_articulos_codigos ADD COLUMN es_masterbox TINYINT(1) DEFAULT 0")
            cursor.execute("ALTER TABLE stk_articulos_codigos ADD COLUMN factor_conversion INT DEFAULT 1")
            print("  Configuración de Master Box agregada a códigos alias.")
        except:
            print("  (Configuración ya existe en stk_articulos_codigos)")

        # 3. Índice para búsquedas rápidas de contenido
        try:
            cursor.execute("CREATE INDEX idx_series_parent ON stk_series (parent_id)")
            print("  Índice idx_series_parent creado.")
        except:
            pass

    print("Infraestructura de Fase 1.4 completada.")

if __name__ == "__main__":
    setup_phase_1_4_masterbox()
