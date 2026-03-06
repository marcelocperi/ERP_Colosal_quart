import os
import random
import datetime
from database import get_db_cursor

def generate_full_cycle(enterprise_id=1):
    print(f"🚀 Iniciando ciclo de generación exhaustiva para Empresa ID: {enterprise_id}")
    
    with get_db_cursor(dictionary=True) as cursor:
        # 0. Asegurar Cuentas de Fondos y Depósitos
        cursor.execute("SELECT id FROM erp_cuentas_fondos WHERE enterprise_id = %s AND tipo = 'EFECTIVO' LIMIT 1", (enterprise_id,))
        caja_id = cursor.fetchone()['id']
        
        cursor.execute("SELECT id FROM stk_depositos WHERE enterprise_id = %s LIMIT 1", (enterprise_id,))
        deposito_id = cursor.fetchone()['id'] if cursor.rowcount > 0 else 1

        # 1. Terceros
        cursor.execute("SELECT id FROM erp_terceros WHERE enterprise_id = %s", (enterprise_id,))
        tercero_ids = [r['id'] for r in cursor.fetchall()]

        # 2. Artículos (con nombres para detalle)
        cursor.execute("SELECT id, nombre FROM stk_articulos WHERE enterprise_id = %s", (enterprise_id,))
        articulos = cursor.fetchall()

        # 3. GENERACIÓN DE TRANSACCIONES
        print("Generando transacciones, pagos, stock y retenciones...")
        
        for i in range(25):
            modulo = 'VENTAS' if i % 2 == 0 else 'COMPRAS'
            tid = random.choice(tercero_ids)
            tipo_cbte = '001' if i < 18 else ('003' if i < 22 else '002')
            fecha = datetime.date.today() - datetime.timedelta(days=random.randint(0, 45))
            nro = 7000 + i
            
            neto = random.uniform(2000, 15000)
            iva = round(neto * 0.21, 2)
            total = neto + iva
            
            # A. Comprobante
            try:
                cursor.execute("""
                    INSERT INTO erp_comprobantes (enterprise_id, modulo, tercero_id, tipo_comprobante, punto_venta, numero, fecha_emision, importe_neto, importe_iva, importe_total, estado_pago)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (enterprise_id, modulo, tid, tipo_cbte, 1, nro, fecha, neto, iva, total, 'PAGADO'))
                cbte_id = cursor.lastrowid
                
                art = random.choice(articulos)
                cursor.execute("""
                    INSERT INTO erp_comprobantes_detalle 
                    (enterprise_id, comprobante_id, articulo_id, descripcion, cantidad, precio_unitario, alicuota_iva, subtotal_neto, importe_iva, subtotal_total)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (enterprise_id, cbte_id, art['id'], art['nombre'], 1.0, neto, 21.0, neto, iva, total))

                # B. Stock
                motivo_id = 1 if modulo == 'VENTAS' else 2
                cursor.execute("""
                    INSERT INTO stk_movimientos (enterprise_id, fecha, motivo_id, deposito_destino_id, comprobante_id, user_id, estado)
                    VALUES (%s, %s, %s, %s, %s, %s, 'CONFIRMADO')
                """, (enterprise_id, fecha, motivo_id, deposito_id, cbte_id, 1))
                mov_id = cursor.lastrowid
                
                cursor.execute("""
                    INSERT INTO stk_movimientos_detalle (enterprise_id, movimiento_id, articulo_id, cantidad)
                    VALUES (%s, %s, %s, %s)
                """, (enterprise_id, mov_id, art['id'], 1))

                # C. Tesorería
                tipo_fondos = 'INGRESO' if modulo == 'VENTAS' else 'EGRESO'
                cursor.execute("""
                    INSERT INTO erp_movimientos_fondos (enterprise_id, fecha, tipo, tercero_id, cuenta_fondo_id, importe, concepto, comprobante_asociado_id, user_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (enterprise_id, fecha, tipo_fondos, tid, caja_id, total, f"Cierre transacción #{nro}", cbte_id, 1))
                
            except Exception as e:
                print(f"❌ Error en ciclo {i}: {e}")

        print("✔ Datos operativos generados exitosamente.")

        # 4. CONTABILIZACIÓN EXHAUSTIVA
        print("Ejecutando centralización contable automática...")
        import flask
        from contabilidad.routes import _generar_asiento_resumen
        
        app = flask.Flask(__name__)
        with app.app_context():
            from flask import g
            g.user = {'id': 1, 'enterprise_id': enterprise_id}
            
            # Centralizar últimos 2 meses
            today = datetime.date.today()
            periods = [(today.year, today.month), 
                      (today.year if today.month > 1 else today.year-1, today.month-1 if today.month > 1 else 12)]
            
            for ay in periods:
                for mod in ['VENTAS', 'COMPRAS', 'FONDOS']:
                    try:
                        aid = _generar_asiento_resumen(mod, ay[1], ay[0])
                        if aid: print(f"  ✅ Centralizado {mod} {ay[1]}/{ay[0]}: Asiento #{aid}")
                    except Exception as e:
                        print(f"  ❌ Error centralizando {mod} {ay[1]}/{ay[0]}: {e}")

    print("🏁 PROCESO FINALIZADO CON ÉXITO.")

if __name__ == "__main__":
    generate_full_cycle(1)
