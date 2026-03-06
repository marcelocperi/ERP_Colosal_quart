import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def force_close():
    conn = pymysql.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("DB_PORT", "3307")),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        database=os.environ.get("DB_NAME", "multi_mcp_db")
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE sys_transaction_logs SET incident_status = 'CLOSED'")
            affected = cursor.rowcount
            print(f"Direct update affected {affected} rows.")
            conn.commit()
            
            cursor.execute("SELECT incident_status, COUNT(*) FROM sys_transaction_logs GROUP BY incident_status")
            print(f"Results: {cursor.fetchall()}")
    finally:
        conn.close()

if __name__ == "__main__":
    force_close()
