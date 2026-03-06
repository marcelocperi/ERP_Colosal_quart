"""
util_generate_sample_data2.py
Genera datos de Ordenes de Compra y Despachos 
"""
import os, sys
sys.path.append(os.getcwd())
try:
    from database import get_db_cursor
except ImportError:
    pass

def generate_oc():
    print("=== Generador de OC de muestra ===")
    enterprise_id = 0

    try:
        with get_db_cursor() as cursor:
            # 1. Crear Orden de Compra de Importacion
            cursor.execute("""
                INSERT INTO cmp_ordenes_compra
                    (enterprise_id, proveedor_id, fecha, fecha_emision, estado, total, es_importacion, moneda, tipo_cambio_valor)
                VALUES (%s, 3, CURRENT_DATE, CURRENT_DATE, 'APROBADA_TESORERIA', 4500.00, 1, 'USD', 950.00)
            """, (enterprise_id,))
            oc_id = cursor.lastrowid
            print(f"[OK] Orden de Compra (Importacion) Creada ID: {oc_id}")

            # 2. Detalles de la OC
            cursor.execute("""
                INSERT INTO cmp_detalles_orden
                    (enterprise_id, orden_id, articulo_id, cantidad_solicitada, cantidad_recibida, precio_unitario, subtotal)
                VALUES (%s, %s, 4934, 100, 0, 45.00, 4500.00)
            """, (enterprise_id, oc_id))
            print(f"[OK] Detalles de OC agregados")

            # 3. Despacho de Aduana vinculado a la OC (imp_despachos)
            cursor.execute("""
                INSERT INTO imp_despachos
                    (enterprise_id, orden_compra_id, numero_despacho, estado, vessel_name,
                     fecha_arribo_estimada, dias_libres_puerto, costo_demora_diario_usd)
                VALUES (%s, %s, '24001IC04000123A', 'PENDIENTE', 'MAERSK SHANGHAI',
                        DATE_ADD(CURRENT_DATE, INTERVAL 5 DAY), 7, 150.00)
            """, (enterprise_id, oc_id))
            despacho_id = cursor.lastrowid
            print(f"[OK] Despacho de Aduana ID: {despacho_id} vinculado a OC {oc_id}")

            print("\n[✓] OC y Despacho generados exitosamente.")

    except Exception as e:
        print(f"\n[ERROR] {e}")


if __name__ == "__main__":
    generate_oc()
