
import mariadb
import json
from database import DB_CONFIG

def check():
    conn = mariadb.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    # Buscamos libros enriquecidos por fuentes de respaldo
    cur.execute("""
        SELECT id, nombre, codigo as isbn, metadata_json 
        FROM stk_articulos 
        WHERE enterprise_id = 1 
        AND (metadata_json LIKE '%Amazon%' OR metadata_json LIKE '%Mercado Libre%')
        LIMIT 5
    """)
    rows = cur.fetchall()
    
    if rows:
        for r in rows:
            meta = json.loads(r['metadata_json'])
            fuente = meta.get('fuente', 'Desconocida')
            print(f"ID: {r['id']} | Título: {r['nombre']} | Fuente: {fuente}")
            print(f"   Cover: {meta.get('cover_url')}")
    else:
        print("Aún no hay libros procesados por las fuentes de respaldo (Amazon/ML).")
    
    conn.close()

if __name__ == "__main__":
    check()
