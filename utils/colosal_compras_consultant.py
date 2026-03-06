
import sys
import os
import json

# Setup de ruta para DB y Servicios
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from database import get_db_cursor
    from services.industrial_costing_service import IndustrialCostingService
    from services.consignment_service import ConsignmentService
except ImportError as e:
    print(f"Error importando módulos base: {e}")
    sys.exit(1)

def show_menu():
    print("\n" + "="*50)
    print("   COLOSAL-COMPRAS: CONSULTOR TÉCNICO STANDALONE")
    print("="*50)
    print("1. [TÉCNICO] Estructura de Tablas Compras (cmp_*)")
    print("2. [TÉCNICO] Algoritmo de Roll-up Industrial")
    print("3. [USUARIO] Calcular Costo de Artículo Producido")
    print("4. [USUARIO] Consultar Stock en Consignación (AUDITORÍA CISA)")
    print("5. [INFO] Leer Manual Técnico (A fidelidad)")
    print("0. Salir")
    print("="*50)
    return input("Seleccione una opción: ")

def show_tables():
    print("\n--- [Estructura de Tablas Compras] ---")
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SHOW TABLES LIKE 'cmp_%'")
        tables = cursor.fetchall()
        for t in tables:
            name = list(t.values())[0]
            print(f"- {name}")

def show_rollup_algo():
    print("\n--- [Algoritmo de Roll-up Industrial MSAC v4] ---")
    print("1. El costo se basa en la tabla `cmp_recetas_bom`.")
    print("2. Si un componente es semielaborado (tiene receta), el cálculo es RECURSIVO.")
    print("3. Si el componente es materia prima, se busca ÚLTIMA RECEPCIÓN o MEJOR PRECIO.")
    print("4. Se aplican las mermas teóricas de `cmp_recetas_detalle`.")
    print("5. Se adicionan gastos de `cmp_articulos_costos_indirectos`.")

def calculate_cost_standalone():
    art_id = input("Ingrese el ID del artículo producido: ")
    try:
        res = IndustrialCostingService.get_industrial_cost(0, int(art_id))
        print(f"\nANÁLISIS DE COSTO PARA ART #{art_id}:")
        print(json.dumps(res, indent=4, ensure_ascii=False))
    except Exception as e:
        print(f"Error al calcular: {e}")

def show_consignment_audit():
    print("\n--- [AUDITORÍA CISA: Stock en Consignación] ---")
    try:
        rows = ConsignmentService.get_stock_en_consignacion(0)
        if not rows:
            print("No hay stock en consignación pendiente.")
        else:
            print(f"{'Tercero':<25} | {'Artículo':<25} | {'Pendiente':<10} | {'Valor Unit.':<10}")
            print("-" * 75)
            for r in rows:
                print(f"{r['tercero'][:24]:<25} | {r['articulo'][:24]:<25} | {r['pendiente']:<10.2f} | {r['valor_unitario']:<10.2f}")
    except Exception as e:
        print(f"Error al auditar: {e}")

def read_manual():
    manual_path = os.path.join(project_root, 'backoffice', 'compras_technical_manual.md')
    if os.path.exists(manual_path):
        with open(manual_path, 'r', encoding='utf-8') as f:
            print("\n" + f.read())
    else:
        print("Manual no encontrado.")

def main():
    while True:
        opt = show_menu()
        if opt == '1': show_tables()
        elif opt == '2': show_rollup_algo()
        elif opt == '3': calculate_cost_standalone()
        elif opt == '4': show_consignment_audit()
        elif opt == '5': read_manual()
        elif opt == '0': break
        else: print("Opción inválida.")

if __name__ == "__main__":
    main()
