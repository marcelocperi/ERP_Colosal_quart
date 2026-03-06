import sys
import os

# Ajustar ruta para importar database y servicios
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from database import get_db_cursor
from decimal import Decimal

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def show_menu():
    print("="*60)
    print("      COLOSAL ERP - CONSULTOR DE VENTAS STANDALONE      ")
    print("="*60)
    print("1. [Técnico] Detalle de Tablas de Ventas (ERD Core)")
    print("2. [Usuario] Top 10 Clientes por Facturación")
    print("3. [Auditor] Consulta de Saldo de Cliente (Cta. Cte.)")
    print("4. [Fiscal]  Listado de Comprobantes Pendientes de CAE")
    print("5. [Manual]  Acceso a Manual Técnico de Ventas MSAC v4")
    print("0. Salir")
    print("="*60)

def show_tables_detail():
    print("\n--- [Estructura de Datos de Ventas (MSAC v4)] ---")
    tables = {
        "erp_terceros": "Maestro de Clientes y Proveedores (es_cliente=1).",
        "erp_comprobantes": "Cabecera de Facturas, NC, ND y Remitos de Salida.",
        "erp_comprobantes_detalle": "Líneas de venta con artículos, cantidades y alícuotas IVA.",
        "erp_comprobantes_impuestos": "Percepciones aplicadas dinámicamente por jurisdicción.",
        "erp_terceros_cm05": "Coeficientes de IIBB para Convenio Multilateral.",
        "fin_factura_cobros": "Pagos registrados al momento de la venta."
    }
    for t, desc in tables.items():
        print(f"  - {t:<26} | {desc}")
    input("\nPresione Enter para continuar...")

def show_top_customers():
    print("\n--- [Top 10 Clientes por Facturación Acumulada] ---")
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT t.nombre, t.cuit, SUM(c.importe_total) as total
            FROM erp_comprobantes c
            JOIN erp_terceros t ON c.tercero_id = t.id
            WHERE c.tipo_operacion = 'VENTA' AND c.tipo_comprobante IN ('001', '006', '011')
            GROUP BY t.id
            ORDER BY total DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        print(f"{'Cliente':<35} | {'CUIT':<15} | {'Total Facturado':>15}")
        print("-" * 70)
        for r in rows:
            print(f"{r['nombre'][:33]:<35} | {r['cuit']:<15} | ${float(r['total']):>14,.2f}")
    input("\nPresione Enter para continuar...")

def calculate_client_balance():
    cuit = input("\nIngrese CUIT del cliente a consultar (sin guiones): ").strip()
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre FROM erp_terceros WHERE cuit LIKE %s AND es_cliente = 1", (f"%{cuit}%",))
        cliente = cursor.fetchone()
        if not cliente:
            print("[!] Cliente no encontrado.")
            return

        # Lógica de Cuenta Corriente resumida
        DEBITO_TIPOS = "('001','002','006','007','011','012','005','010','015')"
        NC_TIPOS     = "('003','008','013')"
        
        cursor.execute(f"""
            SELECT 
                COALESCE(SUM(CASE WHEN tipo_comprobante IN {DEBITO_TIPOS} THEN importe_total ELSE 0 END), 0) -
                COALESCE(SUM(CASE WHEN tipo_comprobante IN {NC_TIPOS} THEN importe_total ELSE 0 END), 0) AS saldo_comp
            FROM erp_comprobantes 
            WHERE tercero_id = %s AND modulo IN ('VEN', 'VENTAS')
        """, (cliente['id'],))
        saldo_comp = float(cursor.fetchone()['saldo_comp'])

        cursor.execute("""
            SELECT COALESCE(SUM(rd.importe), 0) as saldo_recibos 
            FROM fin_recibos_detalles rd 
            JOIN fin_recibos r ON rd.recibo_id = r.id 
            WHERE r.tercero_id = %s
        """, (cliente['id'],))
        saldo_rec = float(cursor.fetchone()['saldo_recibos'])

        print(f"\nEstado de Cuenta para: {cliente['nombre']}")
        print(f"  - Saldo en Comprobantes: ${saldo_comp:>12,.2f}")
        print(f"  - Pagos (Recibos):       ${saldo_rec:>12,.2f}")
        print("-" * 40)
        print(f"  - SALDO TOTAL ADEUDADO:  ${(saldo_comp - saldo_rec):>12,.2f}")

    input("\nPresione Enter para continuar...")

def show_manual_extract():
    manual_path = os.path.join(project_root, 'backoffice', 'ventas_technical_manual.md')
    print("\n--- [Extracto del Manual Técnico de Ventas] ---")
    if os.path.exists(manual_path):
        with open(manual_path, 'r', encoding='utf-8') as f:
            print(f.read())
    else:
        print("[!] Manual no encontrado.")
    input("\nPresione Enter para continuar...")

def main():
    while True:
        clear_screen()
        show_menu()
        opcion = input("Seleccione una opción: ")
        
        if opcion == '1': show_tables_detail()
        elif opcion == '2': show_top_customers()
        elif opcion == '3': calculate_client_balance()
        elif opcion == '4': print("\n[WIP] Proximamente en Phase 1.6 de Facturación Electrónica.") 
        elif opcion == '5': show_manual_extract()
        elif opcion == '0': break
        else: print("Opción no válida.")

if __name__ == "__main__":
    main()
