import mariadb
from database import DB_CONFIG
import json

def dump_all():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    data = {}
    
    cursor.execute('SELECT * FROM sys_external_services')
    data['services'] = cursor.fetchall()
    
    cursor.execute('SELECT * FROM stk_tipos_articulo_servicios')
    data['mappings'] = cursor.fetchall()
    
    with open('services_full_dump.json', 'w') as f:
        json.dump(data, f, indent=2, default=str)
        
    conn.close()

if __name__ == "__main__":
    dump_all()
