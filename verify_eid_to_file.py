from database import get_db_cursor
import sys

TABLES_TO_CHECK = [
    'stk_logisticas',
    'stk_transferencias', 
    'stk_items_transferencia',
    'stk_inventarios',
    'stk_items_inventario'
]

results = []

with get_db_cursor(dictionary=True) as cursor:
    for table in TABLES_TO_CHECK:
        try:
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            
            has_enterprise_id = any(col['Field'] == 'enterprise_id' for col in columns)
            results.append({
                'table': table,
                'has_enterprise_id': has_enterprise_id,
                'column_count': len(columns)
            })
        except Exception as e:
            results.append({
                'table': table,
                'has_enterprise_id': False,
                'error': str(e)
            })

# Write to file
with open('enterprise_id_check_results.txt', 'w', encoding='utf-8') as f:
    f.write("VERIFICACION DE ENTERPRISE_ID\n")
    f.write("="*60 + "\n\n")
    
    for r in results:
        f.write(f"Tabla: {r['table']}\n")
        if 'error' in r:
            f.write(f"  ERROR: {r['error']}\n")
        else:
            status = "SI" if r['has_enterprise_id'] else "NO"
            f.write(f"  Tiene enterprise_id: {status}\n")
            f.write(f"  Total columnas: {r['column_count']}\n")
        f.write("\n")

print("Resultados guardados en: enterprise_id_check_results.txt")

# Summary
total = len(results)
with_eid = sum(1 for r in results if r.get('has_enterprise_id', False))
print(f"\nResumen: {with_eid}/{total} tablas tienen enterprise_id")
