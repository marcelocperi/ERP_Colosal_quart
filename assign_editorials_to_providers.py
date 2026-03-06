"""
assign_editorials_to_providers.py
----------------------------------
Agrupa los artículos de stk_articulos por editorial (campo 'marca'),
y los asigna en round-robin a los proveedores de erp_terceros (ordenados por id)
insertando/actualizando registros en cmp_articulos_proveedores.
"""

import sys, os
sys.path.append(os.path.dirname(__file__))
from database import get_db_cursor

def main():
    with get_db_cursor(dictionary=True) as cursor:

        # 1. Obtener enterprise_id a usar (tomamos el primero disponible, o ajustar)
        cursor.execute("SELECT DISTINCT enterprise_id FROM stk_articulos ORDER BY enterprise_id LIMIT 1")
        row = cursor.fetchone()
        if not row:
            print("No hay artículos en stk_articulos.")
            return
        enterprise_id = row['enterprise_id']
        print(f"Usando enterprise_id: {enterprise_id}")

        # 2. Proveedores ordenados por id
        cursor.execute("""
            SELECT id, nombre, codigo
            FROM erp_terceros
            WHERE es_proveedor = 1
              AND enterprise_id = %s
              AND activo = 1
            ORDER BY id
        """, (enterprise_id,))
        proveedores = cursor.fetchall()

        if not proveedores:
            print("No hay proveedores activos en erp_terceros.")
            return

        print(f"Proveedores encontrados: {len(proveedores)}")
        for p in proveedores:
            print(f"  [{p['id']}] {p['nombre']} (código: {p['codigo']})")

        # 3. Agrupar artículos por editorial (marca)
        cursor.execute("""
            SELECT marca, GROUP_CONCAT(id ORDER BY id) AS ids_str, COUNT(*) AS total
            FROM stk_articulos
            WHERE enterprise_id = %s
              AND marca IS NOT NULL
              AND marca != ''
            GROUP BY marca
            ORDER BY marca
        """, (enterprise_id,))
        editoriales = cursor.fetchall()

        if not editoriales:
            print("No hay artículos con editorial (marca) asignada.")
            return

        print(f"\nEditoriales encontradas: {len(editoriales)}")

        # 4. Asignar en round-robin: cada editorial al siguiente proveedor por id
        num_proveedores = len(proveedores)
        insertados = 0
        actualizados = 0
        omitidos = 0

        for idx, editorial in enumerate(editoriales):
            proveedor = proveedores[idx % num_proveedores]
            ids_articulos = [int(x) for x in editorial['ids_str'].split(',')]

            print(f"\n  Editorial: '{editorial['marca']}' ({editorial['total']} artículos)"
                  f" → Proveedor [{proveedor['id']}] {proveedor['nombre']}")

            for art_id in ids_articulos:
                # Verificar si ya existe la relación
                cursor.execute("""
                    SELECT id FROM cmp_articulos_proveedores
                    WHERE enterprise_id = %s
                      AND articulo_id = %s
                      AND proveedor_id = %s
                """, (enterprise_id, art_id, proveedor['id']))
                existente = cursor.fetchone()

                if existente:
                    omitidos += 1
                else:
                    cursor.execute("""
                        INSERT INTO cmp_articulos_proveedores
                            (enterprise_id, articulo_id, proveedor_id,
                             codigo_articulo_proveedor, es_habitual, user_id)
                        VALUES (%s, %s, %s, %s, 0, 1)
                    """, (enterprise_id, art_id, proveedor['id'], proveedor['codigo']))
                    insertados += 1

        print(f"\n{'='*50}")
        print(f"Proceso completado:")
        print(f"  Insertados : {insertados}")
        print(f"  Ya existían: {omitidos}")
        print(f"{'='*50}")

if __name__ == "__main__":
    main()
