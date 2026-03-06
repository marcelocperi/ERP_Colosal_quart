import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM system_stats")
    stats = cursor.fetchall()
    print("ESTADO DE SYSTEM_STATS:")
    for s in stats:
        print(f"  {s['key_name']}: {s['value_int']} | {s['value_str']}")
    conn.close()
except Exception as e:
    print(f"Error: {e}")
