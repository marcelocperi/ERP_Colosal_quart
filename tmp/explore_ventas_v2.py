import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
sys.path.append(project_root)
from database import get_db_cursor

def explore_ventas():
    with get_db_cursor(dictionary=True) as cursor:
        print("\n--- [Tablas Relacionadas con Ventas] ---")
        cursor.execute("SHOW TABLES")
        tables = [list(t.values())[0] for t in cursor.fetchall()]
        
        ventas_related = [t for t in tables if any(key in t.lower() for key in ['vta', 'venta', 'pedido', 'presupuesto', 'comprobante', 'cliente', 'articulo'])]
        for t in ventas_related:
            print(f"- {t}")

        # Intentar obtener esquemas de las más comunes
        common = ['erp_comprobantes', 'erp_comprobantes_detalle', 'stk_articulos', 'clientes', 'vta_pedidos', 'vta_presupuestos']
        for t in common:
            if t in tables:
                cursor.execute(f"DESCRIBE {t}")
                print(f"\nSCHEMA {t}:")
                for r in cursor.fetchall():
                    print(f"  {r['Field']:<20} | {r['Type']}")
            else:
                print(f"\n[!] Tabla no encontrada: {t}")

if __name__ == "__main__":
    explore_ventas()
