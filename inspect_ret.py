import sys
sys.path.insert(0, '.')
from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("DESCRIBE erp_comprobantes_impuestos")
    print("=== erp_comprobantes_impuestos ===")
    for c in cursor.fetchall():
        print(f"  {c['Field']} - {c['Type']}")
