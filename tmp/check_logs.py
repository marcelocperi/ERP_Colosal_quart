import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def check_all_logs():
    with get_db_cursor() as cursor:
        cursor.execute("SELECT status, incident_status, COUNT(*) FROM sys_transaction_logs GROUP BY status, incident_status")
        results = cursor.fetchall()
        print("Detailed Stats (Status, Incident Status, Count):")
        for r in results:
            print(r)
        
        cursor.execute("SELECT id, incident_status FROM sys_transaction_logs ORDER BY id DESC LIMIT 10")
        latest = cursor.fetchall()
        print("\nLatest 10 records (ID, Incident Status):")
        for l in latest:
            print(l)

if __name__ == "__main__":
    check_all_logs()
