import json

with open('schema_check_report.json', encoding='utf-8') as f:
    r = json.load(f)

missing_tables = {t['table'] for t in r['missing_tables']}
print('TABLAS FALTANTES:', sorted(missing_tables))
print()
print('COLUMNAS ESPERADAS POR TABLA (en código):')
by_table = {}
for col in r['missing_columns']:
    if col['table'] in missing_tables:
        by_table.setdefault(col['table'], []).append(col['column'])

for t, cols in sorted(by_table.items()):
    print(f"  {t}: {cols}")
print()
# También mostrar columnas de tablas existentes que faltan (top 20 por tabla)
print('TOP COLUMNAS FALTANTES EN TABLAS EXISTENTES:')
existing = {}
for col in r['missing_columns']:
    if col['table'] not in missing_tables:
        existing.setdefault(col['table'], []).append((col['column'], col.get('productive')))
for t, cols in sorted(existing.items())[:15]:
    prod_cols = [c for c,p in cols if p]
    print(f"  {t}: {[c for c,_ in cols][:8]}")
