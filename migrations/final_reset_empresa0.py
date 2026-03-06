import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def final_reset_empresa0():
    invoice_ids = [4, 5, 53, 54]
    
    with get_db_cursor(dictionary=True) as cursor:
        print(f"Iniciando REINICIO FINAL de facturas {invoice_ids} para Empresa 0...")
        
        # Obtener los asiento_id antes de borrar
        cursor.execute(f"SELECT asiento_id FROM erp_comprobantes WHERE id IN ({','.join(['%s']*len(invoice_ids))})", tuple(invoice_ids))
        asiento_ids = [r['asiento_id'] for r in cursor.fetchall() if r['asiento_id']]
        
        # Deshabilitar checks de FK
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        try:
            # 1. Detalles de Comprobantes
            cursor.execute(f"DELETE FROM erp_comprobantes_detalle WHERE comprobante_id IN ({','.join(['%s']*len(invoice_ids))})", tuple(invoice_ids))
            print(f"  - erp_comprobantes_detalle: {cursor.rowcount} filas.")

            # 2. Impuestos/Percepciones
            cursor.execute(f"DELETE FROM erp_comprobantes_impuestos WHERE comprobante_id IN ({','.join(['%s']*len(invoice_ids))})", tuple(invoice_ids))
            print(f"  - erp_comprobantes_impuestos: {cursor.rowcount} filas.")

            # 3. Cobros (fin_factura_cobros)
            cursor.execute(f"DELETE FROM fin_factura_cobros WHERE factura_id IN ({','.join(['%s']*len(invoice_ids))})", tuple(invoice_ids))
            print(f"  - fin_factura_cobros: {cursor.rowcount} filas.")

            # 4. Stock
            cursor.execute(f"SELECT id FROM stk_movimientos WHERE comprobante_id IN ({','.join(['%s']*len(invoice_ids))})", tuple(invoice_ids))
            mov_ids = [r['id'] for r in cursor.fetchall()]
            
            if mov_ids:
                format_strings = ','.join(['%s'] * len(mov_ids))
                cursor.execute(f"DELETE FROM stk_movimientos_detalle WHERE movimiento_id IN ({format_strings})", tuple(mov_ids))
                print(f"  - stk_movimientos_detalle: {cursor.rowcount} filas.")
                
            cursor.execute(f"DELETE FROM stk_movimientos WHERE comprobante_id IN ({','.join(['%s']*len(invoice_ids))})", tuple(invoice_ids))
            print(f"  - stk_movimientos: {cursor.rowcount} filas.")

            # 5. Contabilidad
            if asiento_ids:
                format_asientos = ','.join(['%s'] * len(asiento_ids))
                cursor.execute(f"DELETE FROM cont_asientos_detalle WHERE asiento_id IN ({format_asientos})", tuple(asiento_ids))
                print(f"  - cont_asientos_detalle: {cursor.rowcount} filas.")
                
                cursor.execute(f"DELETE FROM cont_asientos WHERE id IN ({format_asientos})", tuple(asiento_ids))
                print(f"  - cont_asientos: {cursor.rowcount} filas.")

            # 6. Comprobantes
            cursor.execute(f"DELETE FROM erp_comprobantes WHERE id IN ({','.join(['%s']*len(invoice_ids))})", tuple(invoice_ids))
            print(f"  - erp_comprobantes: {cursor.rowcount} filas.")

            # 7. Reseteo de existencias de stock (OPCIONAL pero recomendado para un clean start)
            # Como borramos los movimientos, las existencias en stk_existencias podrian quedar mal
            # Si quieres un reset total de stock para Empresa 0:
            # cursor.execute("UPDATE stk_existencias SET cantidad = 0 WHERE enterprise_id = 0")
            # print("  - stk_existencias: Reseteadas a 0 para Empresa 0.")

        except Exception as e:
            print(f"ERROR durante el reinicio: {str(e)}")
            raise e
        finally:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    print("\nReinicio total de Empresa 0 completado. Ahora puedes empezar desde la factura 1.")

if __name__ == '__main__':
    final_reset_empresa0()
