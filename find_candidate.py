
import mariadb
from database import DB_CONFIG

def find_candidate():
    conn = mariadb.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT id, nombre, codigo as isbn 
        FROM stk_articulos 
        WHERE enterprise_id = 1 
        AND (metadata_json IS NULL OR JSON_EXTRACT(metadata_json, '$.cover_url') IS NULL)
        AND LENGTH(codigo) BETWEEN 10 AND 13
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"ID: {r['id']} | Título: {r['nombre']} | ISBN: {r['isbn']}")
    conn.close()

if __name__ == "__main__":
    find_candidate()
