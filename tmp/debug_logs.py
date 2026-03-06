import os
import sys
import json

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def debug_logs():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT incident_status, COUNT(*) as qty FROM sys_transaction_logs GROUP BY incident_status")
        stats = cursor.fetchall()
        print(f"Stats: {stats}")
        
        # Close everything
        cursor.execute("UPDATE sys_transaction_logs SET incident_status = 'CLOSED'")
        print(f"Update done. Rows affected: {cursor.rowcount}")

        cursor.execute("SELECT incident_status, COUNT(*) as qty FROM sys_transaction_logs GROUP BY incident_status")
        stats_after = cursor.fetchall()
        print(f"Stats After: {stats_after}")

if __name__ == "__main__":
    debug_logs()
