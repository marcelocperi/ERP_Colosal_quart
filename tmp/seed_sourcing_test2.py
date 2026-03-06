
import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor
from datetime import datetime, timedelta

def seed_compras_test():
    """
    Crea facturas de compra ficticias para un artículo (ID 1) 
    con dos proveedores distintos para forzar el comportamiento de Sourcing.
    """
    try:
        with get_db_cursor(dictionary=True) as cursor:
            print("--- Creando compras de prueba para Sourcing (Fase 1.2) ---")
            
            # Articulo 1 (Asumimos existe, sino buscar uno)
            cursor.execute("SELECT id FROM stk_articulos WHERE enterprise_id=0 LIMIT 1")
            res_art = cursor.fetchone()
            if not res_art:
                print("No hay artículos en enterprise 0.")
                return
            aid = res_art['id']
            
            # Proveedor 1 y 2
            cursor.execute("SELECT id FROM erp_terceros WHERE es_proveedor = 1 AND (enterprise_id=0 OR enterprise_id=1) LIMIT 2")
            provs = cursor.fetchall()
            if len(provs) < 2:
                print("No hay suficientes proveedores. Cree algunos antes.")
                return

            p1 = provs[0]['id']
            p2 = provs[1]['id']

            # Factura 1 (Mas antigua, Proveedor 1, Precio 100)
            print(f"  > Factura Proveedor {p1} (Precio 100)")
            cursor.execute("""
                INSERT INTO erp_comprobantes (enterprise_id, tercero_id, tipo_comprobante, punto_venta, numero, fecha_emision, importe_total, tipo_operacion, descripcion)
                VALUES (0, %s, 'FACT', 1, 101, %s, 100, 'COMPRA', 'Souring Test 1')
            """, (p1, datetime.now() - timedelta(days=10)))
            c1 = cursor.lastrowid
            cursor.execute("INSERT INTO erp_comprobantes_detalle (comprobante_id, articulo_id, cantidad, precio_unitario, enterprise_id, descripcion) VALUES (%s, %s, 1, 100, 0, 'Sourcing Item 1')", (c1, aid))

            # Factura 2 (Mas reciente, Proveedor 2, Precio 95)
            print(f"  > Factura Proveedor {p2} (Precio 95) - Mas barata y reciente")
            cursor.execute("""
                INSERT INTO erp_comprobantes (enterprise_id, tercero_id, tipo_comprobante, punto_venta, numero, fecha_emision, importe_total, tipo_operacion, descripcion)
                VALUES (0, %s, 'FACT', 1, 102, %s, 95, 'COMPRA', 'Souring Test 2')
            """, (p2, datetime.now() - timedelta(days=5)))
            c2 = cursor.lastrowid
            cursor.execute("INSERT INTO erp_comprobantes_detalle (comprobante_id, articulo_id, cantidad, precio_unitario, enterprise_id, descripcion) VALUES (%s, %s, 1, 95, 0, 'Sourcing Item 1')", (c2, aid))

            print("Compras de prueba creadas exitosamente.")

    except Exception as e:
        print(f"Error seeding test data: {e}")

if __name__ == "__main__":
    seed_compras_test()
