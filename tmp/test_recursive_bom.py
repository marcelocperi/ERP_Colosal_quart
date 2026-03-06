
import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor
from services.industrial_costing_service import IndustrialCostingService

def test_recursive_bom():
    """
    Simulación de Producto Complejo (Recursive BOM):
    - Producto Final (Computadora)
      - Placa Madre (Semielaborado)
        - Chip (Insumo)
        - PCB (Insumo)
      - Gabinete (Insumo)
    """
    try:
        with get_db_cursor(dictionary=True) as cursor:
            print("--- Creando BOM Multi-Nivel (Fase 1.2 Recursiva) ---")
            
            # Artículos
            cursor.execute("SELECT id FROM stk_articulos LIMIT 5")
            arts = cursor.fetchall()
            id_final = arts[0]['id'] # Computadora
            id_semi = arts[1]['id']  # Placa Madre
            id_ins1 = arts[2]['id']  # Chip
            id_ins2 = arts[3]['id']  # PCB
            id_ins3 = arts[4]['id']  # Gabinete

            # Precios de insumos en el maestro
            cursor.execute("INSERT IGNORE INTO cmp_articulos_proveedores (enterprise_id, articulo_id, proveedor_id, origen_id, precio_referencia, es_habitual, user_id) VALUES (0, %s, 1, 1, 100.0, 1, 1)", (id_ins1,))
            cursor.execute("INSERT IGNORE INTO cmp_articulos_proveedores (enterprise_id, articulo_id, proveedor_id, origen_id, precio_referencia, es_habitual, user_id) VALUES (0, %s, 1, 1, 20.0, 1, 1)", (id_ins2,))
            cursor.execute("INSERT IGNORE INTO cmp_articulos_proveedores (enterprise_id, articulo_id, proveedor_id, origen_id, precio_referencia, es_habitual, user_id) VALUES (0, %s, 1, 1, 50.0, 1, 1)", (id_ins3,))

            # Recipe 1: Placa Madre (Semielaborado) = Chip + PCB
            cursor.execute("INSERT INTO cmp_recetas_bom (enterprise_id, producto_id, nombre_variante, user_id) VALUES (0, %s, 'Mother V1', 1)", (id_semi,))
            rid_semi = cursor.lastrowid
            cursor.execute("INSERT INTO cmp_recetas_detalle (receta_id, articulo_id, cantidad, user_id) VALUES (%s, %s, 1, 1)", (rid_semi, id_ins1))
            cursor.execute("INSERT INTO cmp_recetas_detalle (receta_id, articulo_id, cantidad, user_id) VALUES (%s, %s, 1, 1)", (rid_semi, id_ins2))

            # Recipe 2: Computadora (Final) = Placa Madre + Gabinete
            cursor.execute("INSERT INTO cmp_recetas_bom (enterprise_id, producto_id, nombre_variante, user_id) VALUES (0, %s, 'PC V1', 1)", (id_final,))
            rid_final = cursor.lastrowid
            cursor.execute("INSERT INTO cmp_recetas_detalle (receta_id, articulo_id, cantidad, user_id) VALUES (%s, %s, 1, 1)", (rid_final, id_semi))
            cursor.execute("INSERT INTO cmp_recetas_detalle (receta_id, articulo_id, cantidad, user_id) VALUES (%s, %s, 1, 1)", (rid_final, id_ins3))

            print("Probando cálculo industrial Recursivo...")
            res = IndustrialCostingService.get_industrial_cost(0, id_final)
            print(f"RESULTADO: {res}")
            # Esperado: (100 + 20) + 50 = 170 + gastos

    except Exception as e:
        print(f"Error test recursion: {e}")

if __name__ == "__main__":
    test_recursive_bom()
