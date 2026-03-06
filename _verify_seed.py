from database import get_db_cursor

tables = [
    'erp_terceros', 'stk_articulos', 'cmp_recetas_bom', 
    'cmp_articulos_costos_indirectos', 'cmp_rfq_campanas', 
    'stk_depositos', 'sys_documentos_adjuntos', 'prd_proyectos_desarrollo'
]

with get_db_cursor() as cursor:
    print("RESUMEN DE CARGA (Counts):")
    for t in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {t}")
            count = cursor.fetchone()[0]
            print(f" - {t}: {count}")
        except Exception as e:
            print(f" - {t}: ERROR {e}")
