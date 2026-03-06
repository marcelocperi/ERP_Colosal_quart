import mariadb
from database import DB_CONFIG

def get_create_table():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute('SHOW CREATE TABLE movimientos_pendientes')
    ddl = cursor.fetchone()[1]
    with open('ddl_dump.txt', 'w') as f:
        f.write(ddl)
    conn.close()

if __name__ == "__main__":
    get_create_table()
