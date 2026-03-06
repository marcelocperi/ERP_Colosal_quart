import mariadb
from database import DB_CONFIG
import sys

# Force UTF-8 output
if sys.stdout.encoding.lower() != 'utf-8':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass

conn = mariadb.connect(**DB_CONFIG)
cur = conn.cursor(dictionary=True)

# Service efficiency stats
print("\n" + "="*70)
print("ESTADISTICAS DE EFICIENCIA DE SERVICIOS")
print("="*70)

cur.execute('SELECT * FROM service_efficiency ORDER BY fields_provided DESC, hits_count DESC')
rows = cur.fetchall()

if rows:
    print(f"\n{'Servicio':<25} {'Hits':<12} {'Campos':<12} {'Promedio':<12}")
    print("-"*70)
    for r in rows:
        avg = r['fields_provided'] / r['hits_count'] if r['hits_count'] > 0 else 0
        print(f"{r['service_name']:<25} {r['hits_count']:<12} {r['fields_provided']:<12} {avg:<12.2f}")
    print(f"\nTotal servicios registrados: {len(rows)}")
else:
    print("\nNo hay datos de eficiencia aun. Ejecuta el proceso de enriquecimiento primero.")

# Processing status
print("\n" + "="*70)
print("ESTADO DEL PROCESO DE ENRIQUECIMIENTO")
print("="*70)

cur.execute("SELECT * FROM system_stats WHERE key_name IN ('batch_status', 'batch_processed')")
stats = cur.fetchall()

for s in stats:
    if s['key_name'] == 'batch_status':
        print(f"Estado: {s['value_str']}")
    elif s['key_name'] == 'batch_processed':
        print(f"Libros procesados: {s['value_int']}")

# Article statistics
print("\n" + "="*70)
print("ESTADISTICAS DE ARTICULOS (LIBROS)")
print("="*70)

cur.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN api_checked >= 1 THEN 1 ELSE 0 END) as procesados,
        SUM(CASE WHEN JSON_EXTRACT(metadata_json, '$.cover_url') IS NOT NULL THEN 1 ELSE 0 END) as con_portada,
        SUM(CASE WHEN JSON_EXTRACT(metadata_json, '$.descripcion') IS NOT NULL THEN 1 ELSE 0 END) as con_descripcion,
        SUM(CASE WHEN marca IS NOT NULL AND marca != '' THEN 1 ELSE 0 END) as con_editorial,
        SUM(CASE WHEN modelo IS NOT NULL AND modelo != '' THEN 1 ELSE 0 END) as con_autor
    FROM stk_articulos 
    WHERE enterprise_id = 1 AND codigo IS NOT NULL
""")
art_stats = cur.fetchone()

if art_stats and art_stats['total'] > 0:
    total = art_stats['total']
    print(f"\nTotal articulos con ISBN: {total}")
    print(f"Procesados por API: {art_stats['procesados']} ({art_stats['procesados']/total*100:.1f}%)")
    print(f"Con portada: {art_stats['con_portada']} ({art_stats['con_portada']/total*100:.1f}%)")
    print(f"Con descripcion: {art_stats['con_descripcion']} ({art_stats['con_descripcion']/total*100:.1f}%)")
    print(f"Con editorial: {art_stats['con_editorial']} ({art_stats['con_editorial']/total*100:.1f}%)")
    print(f"Con autor: {art_stats['con_autor']} ({art_stats['con_autor']/total*100:.1f}%)")
else:
    print("\nNo hay articulos en la base de datos.")

print("="*70 + "\n")

conn.close()
