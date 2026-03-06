import mysql.connector
from database import DB_CONFIG

def check_table():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("DESCRIBE stk_impresoras_config")
    for row in cursor.fetchall():
        print(row)
    conn.close()

if __name__ == "__main__":
    check_table()
