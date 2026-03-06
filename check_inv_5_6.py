import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def check_invoice_status():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT c.id, c.punto_venta, c.numero, c.tipo_comprobante, c.importe_total, c.estado_pago, c.asiento_id, c.cae
            FROM erp_comprobantes c 
            WHERE c.enterprise_id = 0 AND c.punto_venta = 1 AND c.numero IN (5, 6)
        """)
        invoices = cursor.fetchall()
        
        print(f"Resultados para Facturas 5 y 6 (Empresa 0):")
        for inv in invoices:
            inv_id = inv['id']
            print(f"\n--- Factura {inv['punto_venta']:04d}-{inv['numero']:08d} (ID: {inv_id}) ---")
            print(f"Importe: {inv['importe_total']} | CAE: {inv['cae']} | Estado Pago: {inv['estado_pago']}")
            
            # 1. Contabilidad
            if inv['asiento_id']:
                cursor.execute("SELECT * FROM cont_asientos WHERE id = %s", (inv['asiento_id'],))
                asiento = cursor.fetchone()
                print(f"  [CONTABILIDAD]: Asiento ID #{asiento['id']} del {asiento['fecha']}")
            else:
                print("  [CONTABILIDAD]: No tiene asiento asociado.")

            # 2. Stock
            cursor.execute("SELECT COUNT(*) as cnt FROM stk_movimientos WHERE comprobante_id = %s", (inv_id,))
            stock_cnt = cursor.fetchone()['cnt']
            if stock_cnt > 0:
                print(f"  [STOCK]: Tiene {stock_cnt} movimientos de stock registrados.")
            else:
                print("  [STOCK]: No tiene movimientos de stock.")

            # 3. Fondos / Recibos
            cursor.execute("SELECT id, importe, fecha FROM erp_movimientos_fondos WHERE comprobante_asociado_id = %s", (inv_id,))
            fondos = cursor.fetchall()
            if fondos:
                print(f"  [FONDOS/RECIBOS]: Tiene {len(fondos)} movimientos:")
                for f in fondos:
                    print(f"    - ID: {f['id']} | Fecha: {f['fecha']} | Importe: {f['importe']}")
            else:
                print("  [FONDOS]: No tiene movimientos de fondos (cobros).")

            # 4. Detalle
            cursor.execute("SELECT COUNT(*) as cnt FROM erp_comprobantes_detalle WHERE comprobante_id = %s", (inv_id,))
            detalle_cnt = cursor.fetchone()['cnt']
            print(f"  [DETALLE]: Tiene {detalle_cnt} items en el detalle.")

if __name__ == '__main__':
    check_invoice_status()
