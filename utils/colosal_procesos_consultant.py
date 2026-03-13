import sys
import os

# Ajustar ruta para importar database y servicios
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def show_menu():
    print("="*60)
    print("      COLOSAL ERP - CONSULTOR FUNCIONAL (PROCESOS)      ")
    print("="*60)
    print("1. [Circuito] ¿Cómo hago una Compra / Factura de Proveedor?")
    print("2. [Circuito] ¿Cómo hago una Factura de Venta / Nota de Crédito?")
    print("3. [Proceso]  Explicar Consignación y Fazón (Taller Externo)")
    print("4. [Finanzas] ¿Cómo veo saldos de Clientes y Cobranzas?")
    print("5. [Manual]   Leer Manual Funcional de Procesos (Format Ollama)")
    print("0. Salir")
    print("="*60)

def help_compras():
    print("\n--- [PASO A PASO: COMPRAS] ---")
    print("1. Ve al módulo de Compras y busca la sección 'Proveedores'.")
    print("2. Crea la ficha del proveedor con su CUIT y nombre.")
    print("3. Para cargar mercadería: Abre 'Carga de Comprobantes'.")
    print("4. Ingresa los datos de la factura que te entregó el proveedor.")
    print("5. Detalla los artículos y cantidades. Al grabar, el sistema")
    print("   aumenta tu stock y crea la deuda para pagar después.")
    input("\nPresione Enter para continuar...")

def help_ventas():
    print("\n--- [PASO A PASO: VENTAS] ---")
    print("1. Ve al módulo de Ventas y busca 'Facturación'.")
    print("2. Selecciona al Cliente de la lista.")
    print("3. Carga los artículos (puedes escanear el código de barras).")
    print("4. El sistema elegirá si es Factura A o B automáticamente.")
    print("5. Confirma para que el sistema obtenga el CAE legal de AFIP.")
    print("6. Si el cliente devuelve mercadería, usa 'Nota de Crédito'.")
    input("\nPresione Enter para continuar...")

def help_consignacion():
    print("\n--- [¿QUÉ ES CONSIGNACIÓN Y FAZÓN?] ---")
    print("* CONSIGNACIÓN PROFESIONAL: El proveedor te deja mercadería")
    print("  pero no se la debes hasta que tú reportes la venta.")
    print("* PRODUCCIÓN A FAZÓN (TALLER): Tú envías los materiales")
    print("  (ej: tela) a un taller externo para que ellos armen el")
    print("  producto final (ej: camisa) y te lo devuelvan.")
    print("* El sistema controla cuánta mercadería 'tuya' tiene el taller.")
    input("\nPresione Enter para continuar...")

def help_finanzas():
    print("\n--- [SALDOS Y COBRANZAS] ---")
    print("1. Para ver deuda de un cliente: Entra a su 'Ficha de Cliente'.")
    print("2. La 'Cuenta Corriente' te muestra el saldo neto acumulado.")
    print("3. Si el cliente paga: Usa la pantalla 'Recibos de Cobro'.")
    print("4. Indica cuánto te pagó y con qué medio (Cheque, Efectivo, etc).")
    print("5. El sistema limpia la deuda automáticamente.")
    input("\nPresione Enter para continuar...")

async def read_functional_manual():
    print("\n--- [EXTRACTO DE MANUAL FUNCIONAL DE PROCESOS] ---")
    path = os.path.join(project_root, 'backoffice', 'compras_funcional_manual.md')
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            print(await f.read())
    else:
        print("[!] Manual no encontrado.")
    input("\nPresione Enter para continuar...")

async def main():
    while True:
        clear_screen()
        show_menu()
        opcion = input("Seleccione una opción: ")
        
        if opcion == '1': help_compras()
        elif opcion == '2': help_ventas()
        elif opcion == '3': help_consignacion()
        elif opcion == '4': help_finanzas()
        elif opcion == '5': await read_functional_manual()
        elif opcion == '0': break
        else: print("Opción no válida.")

if __name__ == "__main__":
    import asyncio
    await main()
