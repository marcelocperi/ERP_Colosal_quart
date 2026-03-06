import mariadb
from database import DB_CONFIG

def get_create_table():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute('SHOW CREATE TABLE stock_motivos')
    ddl = cursor.fetchone()[1]
    with open('ddl_dump_motivos.txt', 'w') as f:
        f.write(ddl)
    conn.close()

if __name__ == "__main__":
    get_create_table()
