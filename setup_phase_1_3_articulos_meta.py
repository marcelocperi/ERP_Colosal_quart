import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from multiMCP.database import get_db_cursor

def update_articulos_meta():
    print("Fase 1.3c — Actualizando metadatos del Maestro de Artículos...")
    with get_db_cursor() as cursor:
        # 1. Asegurar requiere_serie
        try:
            cursor.execute("ALTER TABLE stk_articulos ADD COLUMN requiere_serie TINYINT(1) DEFAULT 0")
            print("  Columna requiere_serie agregada.")
        except:
            print("  (requiere_serie ya existe)")
            
        # 2. Agregar tipo_sku para identificar origen preferido
        try:
            cursor.execute("ALTER TABLE stk_articulos ADD COLUMN sku_origen ENUM('PROPIO', 'PROVEEDOR') DEFAULT 'PROPIO'")
            print("  Columna sku_origen agregada.")
        except:
            print("  (sku_origen ya existe)")

    print("Meta-datos de artículos actualizados.")

if __name__ == "__main__":
    update_articulos_meta()
