
import mariadb
from database import DB_CONFIG

def fix_table():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS stk_articulos")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    fix_table()
