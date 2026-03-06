
"""
util_generate_sample_data.py
Genera datos de muestra para el Manual de Usuario.
Ajustado al esquema real de la DB (verificado 2026-02-25).
"""
import os, sys
from werkzeug.security import generate_password_hash

sys.path.append(os.getcwd())
from database import get_db_cursor

def generate_data():
    print("=== Generador de Datos para el Manual de Usuario ===")
    enterprise_id = 0

    try:
        with get_db_cursor() as cursor:
            # --- 1. Resetear Passwords para navegación ---
            new_hash = generate_password_hash("Admin123!")
            cursor.execute(
                "UPDATE sys_users SET password_hash = %s WHERE username IN ('marcelo', 'superadmin')",
                (new_hash,)
            )
            print("[OK] Passwords actualizados a 'Admin123!'")

            # --- 2. Proveedor Extranjero (Importación) ---
            cursor.execute("""
                INSERT IGNORE INTO erp_terceros
                    (enterprise_id, nombre, cuit, naturaleza, es_proveedor, es_proveedor_extranjero,
                     pais_origen, activo, email)
                VALUES (%s, 'Editorial Global Spain', '30-11223344-5', 'PROVEEDOR',
                        1, 1, 'España', 1, 'spain@editorial.com')
            """, (enterprise_id,))
            print(f"[OK] Proveedor Extranjero ID: {cursor.lastrowid}")

            # --- 3. Proveedor Local (Compras locales) ---
            cursor.execute("""
                INSERT IGNORE INTO erp_terceros
                    (enterprise_id, nombre, cuit, naturaleza, es_proveedor, activo, email, tipo_responsable)
                VALUES (%s, 'Distribuidora del Sur S.A.', '30-55667788-2', 'PROVEEDOR',
                        1, 1, 'compras@distribuidora.com', 'Responsable Inscripto')
            """, (enterprise_id,))
            print(f"[OK] Proveedor Local ID: {cursor.lastrowid}")

            # --- 4. Cliente ---
            cursor.execute("""
                INSERT IGNORE INTO erp_terceros
                    (enterprise_id, nombre, cuit, naturaleza, es_cliente, activo, email, tipo_responsable)
                VALUES (%s, 'Librería Central S.A.', '30-77889900-1', 'CLIENTE',
                        1, 1, 'ventas@libreriacentral.com', 'Responsable Inscripto')
            """, (enterprise_id,))
            print(f"[OK] Cliente ID: {cursor.lastrowid}")

            # --- 5. Artículos ---
            productos = [
                ('BK-2024', 'Pack Libros Colección 2024', 'Lote de libros importados', 1500.00, 4500.00),
                ('BK-DF-01', 'Diccionario Filosófico', 'Obra de referencia filosófica', 800.00, 2200.00),
                ('SRV-DIST',  'Servicio de Distribución', 'Distribución de material editorial', 500.00, 1200.00),
            ]
            for p in productos:
                cursor.execute("""
                    INSERT IGNORE INTO stk_articulos
                        (enterprise_id, codigo, nombre, descripcion, costo, precio_venta, activo, es_vendible)
                    VALUES (%s, %s, %s, %s, %s, %s, 1, 1)
                """, (enterprise_id, *p))
            print("[OK] 3 Artículos creados")

            # --- 6. Despacho de Aduana (imp_despachos) ---
            # Columnas verificadas: vessel_name, estado, días_libres_puerto, etc.
            cursor.execute("""
                INSERT INTO imp_despachos
                    (enterprise_id, numero_despacho, estado, vessel_name,
                     fecha_arribo_estimada, dias_libres_puerto, costo_demora_diario_usd)
                VALUES (%s, '24001IC04000123A', 'EN_TRANSITO', 'MAERSK SHANGHAI',
                        DATE_ADD(CURRENT_DATE, INTERVAL 5 DAY), 7, 150.00)
            """, (enterprise_id,))
            despacho_id = cursor.lastrowid
            print(f"[OK] Despacho de Aduana ID: {despacho_id}")

            # --- 7. Factura de Venta de ejemplo ---
            cursor.execute("""
                INSERT INTO erp_comprobantes
                    (enterprise_id, modulo, tipo_operacion, tercero_id, tipo_comprobante,
                     punto_venta, numero, fecha_emision,
                     importe_neto, importe_iva, importe_total, estado_pago)
                SELECT %s, 'VENTAS', 'VENTA', id, '001',
                       1, 501, CURRENT_DATE,
                       10000.00, 2100.00, 12100.00, 'PENDIENTE'
                FROM erp_terceros WHERE nombre = 'Librería Central S.A.' AND enterprise_id = %s LIMIT 1
            """, (enterprise_id, enterprise_id))
            print(f"[OK] Factura de Venta ID: {cursor.lastrowid}")

        print("\n[✓] Todos los datos generados exitosamente.")
        print("    Puede iniciar sesión con usuario 'marcelo' / clave 'Admin123!'")

    except Exception as e:
        print(f"\n[ERROR] {e}")


if __name__ == "__main__":
    generate_data()
