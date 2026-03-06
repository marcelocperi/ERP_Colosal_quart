from database import get_db_cursor

stmts = [
    # Ajustar cmp_articulos_costos_indirectos
    "ALTER TABLE cmp_articulos_costos_indirectos ADD COLUMN IF NOT EXISTS descripcion VARCHAR(255)",
    "ALTER TABLE cmp_articulos_costos_indirectos ADD COLUMN IF NOT EXISTS base_calculo ENUM('UNIDAD', 'BATCH') DEFAULT 'UNIDAD'",
    "ALTER TABLE cmp_articulos_costos_indirectos ADD COLUMN IF NOT EXISTS cantidad_batch DECIMAL(15,4) DEFAULT 1",
    "ALTER TABLE cmp_articulos_costos_indirectos MODIFY COLUMN tipo_gasto ENUM('MANO_OBRA','ENERGIA','AMORTIZACION','LOGISTICA','CERTIFICACION','CONTROL_CALIDAD','ENSAYOS','OTROS') NOT NULL",
    
    # Asegurar que las tablas de RFQ y Proyectos tienen las columnas correctas
    "ALTER TABLE cmp_recetas_bom CHANGE COLUMN IF EXISTS nombre nombre_variante VARCHAR(100)", # Just in case it was 'nombre'
    
    # Crear tablas si faltan (re-run fix_tables logic simplified)
]

with get_db_cursor() as cursor:
    for sql in stmts:
        try:
            print(f"Running: {sql[:50]}...")
            cursor.execute(sql)
            print("Done.")
        except Exception as e:
            print(f"FAILED (may already be ok): {e}")
