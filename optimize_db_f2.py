import mariadb
from database import DB_CONFIG

def apply_virtual_columns():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("--- Fase 2: Columnas Virtuales e Índices JSON ---")
        
        # 1. Columna Virtual para GÉNERO
        print("Creando columna virtual para GÉNERO...")
        try:
            cursor.execute("""
                ALTER TABLE stk_articulos 
                ADD COLUMN v_genero VARCHAR(100) AS (JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.genero'))) VIRTUAL
            """)
            cursor.execute("CREATE INDEX idx_articulos_v_genero ON stk_articulos(enterprise_id, v_genero)")
            print("  [OK] Columna v_genero e índice creados.")
        except Exception as e: print(f"  [X] {e}")

        # 2. Columna Virtual para PÁGINAS (Numérico para ordenación)
        print("Creando columna virtual para PÁGINAS...")
        try:
            cursor.execute("""
                ALTER TABLE stk_articulos 
                ADD COLUMN v_paginas INT AS (CAST(JSON_EXTRACT(metadata_json, '$.paginas') AS UNSIGNED)) VIRTUAL
            """)
            cursor.execute("CREATE INDEX idx_articulos_v_paginas ON stk_articulos(enterprise_id, v_paginas)")
            print("  [OK] Columna v_paginas e índice creados.")
        except Exception as e: print(f"  [X] {e}")

        # 3. Columna Virtual para FECHA PUBLICACIÓN
        print("Creando columna virtual para FECHA_PUB...")
        try:
            cursor.execute("""
                ALTER TABLE stk_articulos 
                ADD COLUMN v_fecha_pub VARCHAR(20) AS (JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.fecha_publicacion'))) VIRTUAL
            """)
            cursor.execute("CREATE INDEX idx_articulos_v_fecha ON stk_articulos(enterprise_id, v_fecha_pub)")
            print("  [OK] Columna v_fecha_pub e índice creados.")
        except Exception as e: print(f"  [X] {e}")

        conn.commit()
        print("\n¡Fase 2 de optimización completada!")
        conn.close()
    except Exception as e:
        print(f"Error crítico en Fase 2: {e}")

if __name__ == "__main__":
    apply_virtual_columns()
