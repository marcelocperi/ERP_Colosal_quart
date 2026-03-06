
import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor
from services.industrial_costing_service import IndustrialCostingService

def seed_industrial_test():
    """
    Crea un producto producido ("Placa Electrónica") con:
    - 2 Insumos (Materiales)
    - 2 Gastos Indirectos (Mano de Obra y Energía)
    - 1 Margen de Ganancia del 35%
    """
    try:
        with get_db_cursor(dictionary=True) as cursor:
            print("--- Creando Producto Industrial de Prueba ---")
            
            # 1. Artículos (Padre e Insumos)
            cursor.execute("SELECT id FROM stk_articulos WHERE enterprise_id=0 LIMIT 3")
            arts = cursor.fetchall()
            if len(arts) < 3:
                print("No hay suficientes artículos.")
                return

            padre_id = arts[0]['id']
            insumo1 = arts[1]['id']
            insumo2 = arts[2]['id']

            # Seed sourcing for insumos (Fase 1.1/1.2)
            cursor.execute("INSERT IGNORE INTO cmp_articulos_proveedores (enterprise_id, articulo_id, proveedor_id, origen_id, precio_referencia, es_habitual, user_id) VALUES (0, %s, 44, 1, 50.0, 1, 1)", (insumo1,))
            cursor.execute("INSERT IGNORE INTO cmp_articulos_proveedores (enterprise_id, articulo_id, proveedor_id, origen_id, precio_referencia, es_habitual, user_id) VALUES (0, %s, 76, 3, 20.0, 1, 1)", (insumo2,))

            # 2. Receta (BOM)
            cursor.execute("INSERT INTO cmp_recetas_bom (enterprise_id, producto_id, nombre_variante, user_id) VALUES (0, %s, 'Standard V1', 1)", (padre_id,))
            rid = cursor.lastrowid
            
            # Detalle BOM (2u Insumo 1, 5u Insumo 2 - este ultimo consignado)
            cursor.execute("INSERT INTO cmp_recetas_detalle (receta_id, articulo_id, cantidad, es_consignado, user_id) VALUES (%s, %s, 2, 0, 1)", (rid, insumo1))
            cursor.execute("INSERT INTO cmp_recetas_detalle (receta_id, articulo_id, cantidad, es_consignado, user_id) VALUES (%s, %s, 5, 1, 1)", (rid, insumo2))

            # 3. Gastos Indirectos (Fase 1.3)
            cursor.execute("""
                INSERT INTO cmp_articulos_costos_indirectos (enterprise_id, articulo_id, tipo_gasto, monto_estimado, porcentaje_margen_esperado, user_id)
                VALUES (0, %s, 'MANO_OBRA', 15.0, 35.0, 1)
            """, (padre_id,))
            cursor.execute("""
                INSERT INTO cmp_articulos_costos_indirectos (enterprise_id, articulo_id, tipo_gasto, monto_estimado, porcentaje_margen_esperado, user_id)
                VALUES (0, %s, 'ENERGIA', 5.0, 35.0, 1)
            """, (padre_id,))

            print("Probando cálculo industrial (Roll-up)...")
            res = IndustrialCostingService.get_industrial_cost(0, padre_id)
            print(f"RESULTADO: {res}")

    except Exception as e:
        print(f"Error seeding industrial data: {e}")

if __name__ == "__main__":
    seed_industrial_test()
