import mariadb
from database import DB_CONFIG
import json

def dump():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT * FROM sys_external_services')
    results = cursor.fetchall()
    with open('services_dump.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    conn.close()

if __name__ == "__main__":
    dump()
