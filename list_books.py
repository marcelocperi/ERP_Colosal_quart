
import mariadb
from database import DB_CONFIG

def list_books():
    conn = mariadb.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, nombre, codigo as isbn FROM stk_articulos WHERE enterprise_id=1 LIMIT 50")
    for r in cur.fetchall():
        print(f"ID: {r['id']} | {r['nombre']} | {r['isbn']}")
    conn.close()

if __name__ == "__main__":
    list_books()
