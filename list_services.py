import mariadb
from database import DB_CONFIG

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, nombre, tipo_servicio, clase_implementacion, activo FROM sys_external_services")
    for r in cursor.fetchall():
        print(r)
    conn.close()
except Exception as e:
    print(e)
