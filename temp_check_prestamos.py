import mariadb
from database import DB_CONFIG

conn = mariadb.connect(**DB_CONFIG)
cursor = conn.cursor()
cursor.execute('DESCRIBE prestamos')
for col in cursor.fetchall():
    print(col[0])
conn.close()
