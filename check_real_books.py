
import mariadb
import json
from database import DB_CONFIG

def check():
    conn = mariadb.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    
    # Buscamos libros con ID bajo que suelen ser reales
    cur.execute("""
        SELECT id, nombre, codigo as isbn, metadata_json 
        FROM stk_articulos 
        WHERE id IN (1, 3, 5, 10, 20)
    """)
    rows = cur.fetchall()
    
    for r in rows:
        meta = json.loads(r['metadata_json']) if r['metadata_json'] else {}
        print(f"--- ID: {r['id']} ---")
        print(f"Título: {r['nombre']}")
        print(f"Portada: {'✓' if meta.get('cover_url') else '✗'}")
        print(f"Descripción: {'✓' if meta.get('descripcion') else '✗'}")
        if meta.get('fuente'):
            print(f"Fuente de Respaldo: {meta.get('fuente')}")
    
    conn.close()

if __name__ == "__main__":
    check()
