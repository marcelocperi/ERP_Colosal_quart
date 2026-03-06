import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def delete_invoices_5_6():
    invoice_ids = [55, 56]
    asiento_ids = [11, 12]
    
    with get_db_cursor() as cursor:
        print(f"Iniciando eliminación de facturas {invoice_ids} y asientos {asiento_ids}...")
        
        # Deshabilitar checks de FK
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        try:
            # 1. Detalles de Comprobantes
            cursor.execute("DELETE FROM erp_comprobantes_detalle WHERE comprobante_id IN (%s, %s)", tuple(invoice_ids))
            print(f"  - erp_comprobantes_detalle: {cursor.rowcount} filas.")

            # 2. Impuestos/Percepciones
            cursor.execute("DELETE FROM erp_comprobantes_impuestos WHERE comprobante_id IN (%s, %s)", tuple(invoice_ids))
            print(f"  - erp_comprobantes_impuestos: {cursor.rowcount} filas.")

            # 3. Cobros (fin_factura_cobros)
            cursor.execute("DELETE FROM fin_factura_cobros WHERE factura_id IN (%s, %s)", tuple(invoice_ids))
            print(f"  - fin_factura_cobros: {cursor.rowcount} filas.")

            # 4. Stock - Primero buscar IDs de movimientos
            cursor.execute("SELECT id FROM stk_movimientos WHERE comprobante_id IN (%s, %s)", tuple(invoice_ids))
            mov_ids = [r[0] for r in cursor.fetchall()]
            
            if mov_ids:
                format_strings = ','.join(['%s'] * len(mov_ids))
                cursor.execute(f"DELETE FROM stk_movimientos_detalle WHERE movimiento_id IN ({format_strings})", tuple(mov_ids))
                print(f"  - stk_movimientos_detalle: {cursor.rowcount} filas.")
                
            cursor.execute("DELETE FROM stk_movimientos WHERE comprobante_id IN (%s, %s)", tuple(invoice_ids))
            print(f"  - stk_movimientos: {cursor.rowcount} filas.")

            # 5. Contabilidad
            cursor.execute("DELETE FROM cont_asientos_detalle WHERE asiento_id IN (%s, %s)", tuple(asiento_ids))
            print(f"  - cont_asientos_detalle: {cursor.rowcount} filas.")
            
            cursor.execute("DELETE FROM cont_asientos WHERE id IN (%s, %s)", tuple(asiento_ids))
            print(f"  - cont_asientos: {cursor.rowcount} filas.")

            # 6. Finalmente, el Comprobante
            cursor.execute("DELETE FROM erp_comprobantes WHERE id IN (%s, %s)", tuple(invoice_ids))
            print(f"  - erp_comprobantes: {cursor.rowcount} filas.")

        except Exception as e:
            print(f"ERROR durante la eliminación: {str(e)}")
            raise e
        finally:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    print("\nEliminación completada con éxito.")

if __name__ == '__main__':
    delete_invoices_5_6()
