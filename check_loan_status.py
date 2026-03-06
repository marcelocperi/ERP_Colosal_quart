
import mariadb
from database import DB_CONFIG

def check_loan_status():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("\nCHECKING ACTIVE LOANS FOR ENT 1:")
    cursor.execute("""
        SELECT id, fecha_devolucion_real 
        FROM prestamos 
        WHERE enterprise_id = 1
    """)
    rows = cursor.fetchall()
    active = 0
    returned = 0
    for r in rows:
        status = "ACTIVE" if r[1] is None else "RETURNED"
        print(f"Loan {r[0]}: {status} (Real Dev: {r[1]})")
        if r[1] is None: active += 1
        else: returned += 1
        
    print(f"\nStats: Active={active}, Returned={returned}")
    conn.close()

if __name__ == "__main__":
    check_loan_status()
