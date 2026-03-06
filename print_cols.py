import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("DESCRIBE stk_movimientos")
    cols = [row[0] for row in cursor.fetchall()]
    print("Columns in stk_movimientos:", cols)
    conn.close()
except Exception as e:
    print(f"Error: {e}")
