import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def investigate_payments():
    with get_db_cursor(dictionary=True) as cursor:
        print("--- Table fin_medios_pago ---")
        cursor.execute("DESCRIBE fin_medios_pago")
        for c in cursor.fetchall():
            print(c)
            
        print("\n--- Example Data fin_medios_pago ---")
        cursor.execute("SELECT * FROM fin_medios_pago LIMIT 5")
        for r in cursor.fetchall():
            print(r)
            
        print("\n--- Checking erp_terceros for payment fields ---")
        cursor.execute("DESCRIBE erp_terceros")
        for c in cursor.fetchall():
            if "pago" in c['Field'] or "medio" in c['Field']:
                print(c)

if __name__ == "__main__":
    investigate_payments()
