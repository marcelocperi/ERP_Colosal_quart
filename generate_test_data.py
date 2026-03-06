import os
import random
import datetime
import json
from database import get_db_cursor

def generate_data(enterprise_id=1):
    print(f"--- GENERANDO DATA PARA EMPRESA ID {enterprise_id} ---")
    
    with get_db_cursor() as cursor:
        # 1. Crear Terceros (Clientes y Proveedores)
        terceros = [
            ("Librería El Ateneo", "30543210987", "Responsable Inscripto", 1, 0),
            ("Papelera del Sur", "30112223334", "Responsable Inscripto", 0, 1),
            ("Juan Pérez (Cliente)", "20304050607", "Consumidor Final", 1, 0),
            ("Distribuidora Global", "30998887776", "Responsable Inscripto", 1, 1),
            ("Tech Solutions S.A.", "30776655443", "Responsable Inscripto", 0, 1),
            ("Ana García (Monotributo)", "27334445556", "Monotributo", 1, 0),
        ]
        
        tercero_ids = []
        for nombre, cuit, cond, es_c, es_p in terceros:
            cursor.execute("""
                INSERT INTO erp_terceros (enterprise_id, nombre, cuit, tipo_responsable, es_cliente, es_proveedor)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (enterprise_id, nombre, cuit, cond, es_c, es_p))
            tercero_ids.append((cursor.lastrowid, es_c, es_p))
        
        print(f"Inserted {len(tercero_ids)} terceros.")

        # 2. Crear Artículos
        articulos = [
            ("Libro: Cien Años de Soledad", "STK-001", 5000, 7500),
            ("Libro: Rayuela", "STK-002", 4500, 6800),
            ("Resma de Hojas A4", "STK-003", 800, 1200),
            ("Marcador Negro", "STK-004", 150, 250),
            ("Notebook Office 15", "STK-005", 250000, 400000),
        ]
        
        articulo_ids = []
        for nombre, codigo, costo, precio in articulos:
            cursor.execute("""
                INSERT INTO stk_articulos (enterprise_id, nombre, codigo, precio_costo, precio_venta)
                VALUES (%s, %s, %s, %s, %s)
            """, (enterprise_id, nombre, codigo, costo, precio))
            articulo_ids.append((cursor.lastrowid, costo, precio))

        print(f"Inserted {len(articulo_ids)} articulos.")

        # 3. Generar Comprobantes (Facturas, NC, ND)
        today = datetime.date.today()
        
        for i in range(50): # 50 transacciones random
            fecha = today - datetime.timedelta(days=random.randint(0, 90))
            tercero = random.choice(tercero_ids)
            tid, is_client, is_prov = tercero
            
            # Decidir si es venta o compra
            if is_client and (not is_prov or random.random() > 0.5):
                modulo = 'VENTAS'
                tipo_cbte = random.choices(['001', '006', '003', '002'], weights=[40, 40, 10, 10])[0] 
                # 001: Fac A, 006: Fac B, 003: NC, 002: ND
            else:
                modulo = 'COMPRAS'
                tipo_cbte = random.choices(['001', '003', '002'], weights=[80, 10, 10])[0]

            # Si es NC o ND, tratar de que sea por diferencia de precio o devolucion
            es_nc_nd = tipo_cbte in ['003', '002', '008', '013']
            motivo = random.choice(['DEVOLUCION', 'DIFERENCIA PRECIO']) if es_nc_nd else None
            
            # Calcular montos
            neto = random.uniform(1000, 50000)
            iva = neto * 0.21
            total = neto + iva
            
            cursor.execute("""
                INSERT INTO erp_comprobantes (enterprise_id, modulo, tercero_id, tipo_comprobante, punto_venta, numero, fecha_emision, importe_neto, importe_iva, importe_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (enterprise_id, modulo, tid, tipo_cbte, 1, 1000 + i, fecha, neto, iva, total))
            cbte_id = cursor.lastrowid
            
            # Items (Simplificado)
            art = random.choice(articulo_ids)
            cursor.execute("""
                INSERT INTO erp_comprobantes_detalle (comprobante_id, articulo_id, cantidad, precio_unitario, alicuota_iva, subtotal_neto, importe_iva)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (cbte_id, art[0], 1, neto, 21.0, neto, iva))

            # 4. Remitos (Stock Movements)
            if tipo_cbte in ['001', '006']: # Facturas generan remito implicito o movimiento
                tipo_mov = 'egreso' if modulo == 'VENTAS' else 'ingreso'
                cursor.execute("""
                    INSERT INTO stk_movimientos (enterprise_id, fecha, tipo, articulo_id, cantidad, deposito_id, concepto)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (enterprise_id, fecha, tipo_mov, art[0], 1, 1, f"Remito {modulo} Fac {tipo_cbte} #{1000+i}"))
                
        print("Fin de generación masiva.")

if __name__ == "__main__":
    generate_data(1)
