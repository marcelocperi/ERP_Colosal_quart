import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("""
        SELECT *
        FROM sys_transaction_logs
        WHERE status = 'ERROR' OR severity >= 7
        ORDER BY id DESC
        LIMIT 10
    """)
    rows = cursor.fetchall()
    
    def default_serializer(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return str(obj)

    with open('last_errors_detailed.json', 'w', encoding='utf-8') as f:
        json.dump(rows, f, indent=4, default=default_serializer)
