import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT enterprise_id, api_checked, COUNT(*) FROM stk_articulos GROUP BY enterprise_id, api_checked")
    counts = cursor.fetchall()
    print("Conteos de enterprise_id y api_checked:")
    for ent, api, count in counts:
        print(f"  Ent={ent}, API={api}: {count}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
