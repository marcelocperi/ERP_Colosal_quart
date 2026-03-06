from database import get_db_cursor
import os

filepath = r"C:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\_schema_dump.txt"
with get_db_cursor() as cursor:
    with open(filepath, 'w', encoding='utf-8') as f:
        for table in ['cmp_recetas_bom', 'cmp_articulos_costos_indirectos', 'stk_articulos', 'erp_terceros']:
            try:
                cursor.execute(f"SHOW CREATE TABLE {table}")
                f.write(f"TABLE: {table}\n{cursor.fetchone()[1]}\n\n")
            except Exception as e:
                f.write(f"TABLE: {table} FAILED: {str(e)}\n\n")
