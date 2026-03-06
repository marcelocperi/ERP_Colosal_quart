import sys
import os
sys.path.insert(0, 'C:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP')
from database import get_db_cursor
try:
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("""
            SELECT 
                status as failure_mode,
                COUNT(*) as total,
                MAX(5) as max_sev
            FROM sys_transaction_logs
            WHERE status != 'SUCCESS'
            GROUP BY status
            LIMIT 5
        """)
        failure_modes = cursor.fetchall()
        print("FAILURE_MODES_DATA:", failure_modes)
except Exception as e:
    print("ERROR:", e)
