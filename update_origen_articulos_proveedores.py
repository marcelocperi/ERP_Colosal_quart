"""
update_origen_articulos_proveedores.py
---------------------------------------
Actualiza el campo origen_id en cmp_articulos_proveedores.

Regla:
  - JSON origen del artículo == 'argentina'  →  id del origen 'Local'
  - cualquier otro valor (o vacío)           →  id del origen 'Importado'
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from database import get_db_cursor

PAISES_LOCALES = ('argentina', 'arg')

def main():
    # ── PASO 1: Buscar los IDs desde cmp_sourcing_origenes
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre FROM cmp_sourcing_origenes WHERE activo = 1 ORDER BY id ASC")
        origenes = cursor.fetchall()

    id_local     = next((o['id'] for o in origenes if 'local'     in o['nombre'].lower()), None)
    id_importado = next((o['id'] for o in origenes if 'importado' in o['nombre'].lower()), None)
    
    if id_importado is None:
        id_importado = next((o['id'] for o in origenes if o['id'] != id_local), None)

    if id_local is None or id_importado is None:
        print("Error: No se encontraron orígenes 'Local' e 'Importado' en cmp_sourcing_origenes.")
        return

    print(f"Mapeando: Local={id_local}, Importado={id_importado}")

    # ── PASO 2: Un solo UPDATE con MariaDB
    with get_db_cursor() as cursor:
        cursor.execute(f"""
            UPDATE cmp_articulos_proveedores ap
            JOIN stk_articulos a 
              ON ap.articulo_id = a.id AND ap.enterprise_id = a.enterprise_id
            SET ap.origen_id = CASE
                WHEN LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(a.metadata_json, '$.origen')))) IN ('argentina', 'arg') THEN {id_local}
                ELSE {id_importado}
            END
        """)
        
        print(f"¡Actualización exitosa! Filas afectadas: {cursor.rowcount}")

if __name__ == "__main__":
    main()
