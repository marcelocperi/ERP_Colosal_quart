
from database import get_db_cursor
from datetime import datetime, timedelta

def seed_industrial_data():
    enterprise_id = 1
    user_id = 1

    with get_db_cursor() as cursor:
        print("🌱 Iniciando Carga de Datos de Prueba - Masterplan Industrial...")

        # 1. PROVEEDORES
        print("   -> erp_terceros")
        proveedores = [
            (enterprise_id, 'EuroGluten SpA (IMP)', None, 'EX-12345', 1, 1),
            (enterprise_id, 'Molinos del Sur S.A. (LOC)', '30-11111111-9', None, 1, 0),
            (enterprise_id, 'Taller Textil & Envase S.R.L.', '30-22222222-8', None, 1, 0)
        ]
        prov_ids = []
        for p in proveedores:
            print(f"      * {p[1]}")
            cursor.execute("""
                INSERT IGNORE INTO erp_terceros (enterprise_id, nombre, cuit, identificador_fiscal, es_proveedor, es_proveedor_extranjero) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, p)
            if p[2]: 
                cursor.execute("SELECT id FROM erp_terceros WHERE cuit = %s", (p[2],))
            else: 
                cursor.execute("SELECT id FROM erp_terceros WHERE identificador_fiscal = %s", (p[3],))
            prov_ids.append(cursor.fetchone()[0])

        eur_id, mol_id, tal_id = prov_ids

        # 2. ARTICULOS
        print("   -> stk_articulos")
        # tipo_articulo: mercaderia, servicio, insumo, activo_fijo
        articulos = [
            (enterprise_id, 'HAR-IMP-01', 'Harina Premium 0000 (Importada)', 'KGR', 120.50, 'insumo'),
            (enterprise_id, 'LEV-LOC-01', 'Levadura Fresca Nacional', 'KGR', 45.00, 'insumo'),
            (enterprise_id, 'ENV-EXP-01', 'Envase Exportación Kraft', 'UN', 15.00, 'insumo'),
            (enterprise_id, 'PAN-PREM-01', 'Pan Artesanal Premium', 'UN', 550.00, 'mercaderia')
        ]
        art_ids = []
        for a in articulos:
            print(f"      * {a[1]}")
            cursor.execute("""
                INSERT IGNORE INTO stk_articulos (enterprise_id, codigo, nombre, unidad_medida, costo, tipo_articulo) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, a)
            cursor.execute("SELECT id FROM stk_articulos WHERE codigo = %s", (a[1],))
            art_ids.append(cursor.fetchone()[0])

        har_id, lev_id, env_id, pan_id = art_ids

        # 3. RECETA (BOM)
        print("   -> cmp_recetas_bom")
        # Columna nombre_variante
        cursor.execute("INSERT IGNORE INTO cmp_recetas_bom (enterprise_id, producto_id, nombre_variante, version, activo) VALUES (%s, %s, 'Receta Standard 2025', 'V1.0', 1)", (enterprise_id, pan_id))
        cursor.execute("SELECT id FROM cmp_recetas_bom WHERE producto_id = %s ORDER BY id DESC LIMIT 1", (pan_id,))
        rec_id = cursor.fetchone()[0]

        print("      * Detalle BOM")
        bom_items = [(rec_id, har_id, 0.600, 5.0), (rec_id, lev_id, 0.050, 2.0), (rec_id, env_id, 1.000, 0.0)]
        for bi in bom_items:
            cursor.execute("INSERT IGNORE INTO cmp_recetas_detalle (receta_id, articulo_id, cantidad, porcentaje_merma_esperada) VALUES (%s, %s, %s, %s)", bi)

        # 4. OVERHEAD
        print("   -> Overhead")
        # tipo_gasto: MANO_OBRA, ENERGIA, AMORTIZACION, LOGISTICA, CERTIFICACION, CONTROL_CALIDAD, ENSAYOS, OTROS
        overhead_items = [
            (enterprise_id, pan_id, 'MANO_OBRA', 'Maestro Panadero', 1500.00, 'BATCH', 100, user_id),
            (enterprise_id, pan_id, 'ENERGIA', 'Horno', 80.00, 'UNIDAD', 1, user_id),
            (enterprise_id, pan_id, 'CONTROL_CALIDAD', 'Bromatologia', 5000.00, 'BATCH', 500, user_id)
        ]
        for oh in overhead_items:
            print(f"      * {oh[3]}")
            cursor.execute("""
                INSERT IGNORE INTO cmp_articulos_costos_indirectos 
                (enterprise_id, articulo_id, tipo_gasto, descripcion, monto_estimado, base_calculo, cantidad_batch, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, oh)

        # 5. RFQ
        print("   -> RFQ")
        fecha_cierre = datetime.now() + timedelta(days=15)
        print("      * Campaña")
        cursor.execute("""
            INSERT IGNORE INTO cmp_rfq_campanas 
            (enterprise_id, fecha_cierre, estado, articulo_objetivo_id, cantidad_objetivo, user_id)
            VALUES (%s, %s, 'ENVIADA', %s, %s, %s)
        """, (enterprise_id, fecha_cierre, pan_id, 1000, user_id))
        cursor.execute("SELECT id FROM cmp_rfq_campanas WHERE articulo_objetivo_id = %s ORDER BY id DESC LIMIT 1", (pan_id,))
        rfq_id = cursor.fetchone()[0]

        print("      * Detalles RFQ")
        rfq_detalles = [(rfq_id, har_id, 630.0, 'EXPLOSION_BOM'), (rfq_id, lev_id, 51.0, 'EXPLOSION_BOM')]
        for rd in rfq_detalles:
            cursor.execute("INSERT IGNORE INTO cmp_rfq_detalles (rfq_id, articulo_insumo_id, cantidad_requerida, sugerencia_origen) VALUES (%s, %s, %s, %s)", rd)

        # 6. FAZÓN
        print("   -> Fazón")
        print("      * Deposito")
        cursor.execute("""
            INSERT IGNORE INTO stk_depositos (enterprise_id, nombre, tipo, tipo_propiedad, tercero_id, user_id)
            VALUES (%s, 'Taller Envases - Fazón', 'SATELITE', 'FAZON_TERCERO', %s, %s)
        """, (enterprise_id, tal_id, user_id))
        cursor.execute("SELECT id FROM stk_depositos WHERE nombre LIKE '%Fazón%'")
        dep_fazon_id = cursor.fetchone()[0]

        print("      * Stock")
        cursor.execute("""
            INSERT IGNORE INTO stk_existencias (enterprise_id, deposito_id, articulo_id, cantidad)
            VALUES (%s, %s, %s, 10000)
        """, (enterprise_id, dep_fazon_id, env_id))

        # 7. DOCUMENTACIÓN
        print("   -> Documentacion")
        doc_vencimiento = datetime.now() + timedelta(days=365)
        documentos = [
            (enterprise_id, 'ARTICULO', har_id, 'CERTIFICADO', 'Harina.pdf', '/uploads/harina.pdf', doc_vencimiento, 'VIGENTE', user_id),
            (enterprise_id, 'PROVEEDOR', eur_id, 'CONTRATO', 'Fazon.pdf', '/web/fazon.pdf', None, 'VIGENTE', user_id)
        ]
        for d in documentos:
            print(f"      * {d[4]}")
            cursor.execute("""
                INSERT IGNORE INTO sys_documentos_adjuntos 
                (enterprise_id, entidad_tipo, entidad_id, tipo_documento, nombre_archivo, ruta_almacenamiento, fecha_vencimiento, estado, user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, d)

        # 8. PROYECTOS I+D
        print("   -> Proyectos I+D")
        cursor.execute("""
            INSERT IGNORE INTO prd_proyectos_desarrollo 
            (enterprise_id, codigo_proyecto, nombre, descripcion, estado, fecha_inicio, presupuesto_estimado, user_id)
            VALUES (%s, 'ID-2025-001', 'Línea Gourmet', 'Investigación materias primas.', 'I_D', %s, 500000.00, %s)
        """, (enterprise_id, datetime.now(), user_id))

        print("\n✅ Carga finalizada con éxito. Circuito industrial completo sembrado.")

if __name__ == "__main__":
    seed_industrial_data()
