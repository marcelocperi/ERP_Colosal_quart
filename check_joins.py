
import mariadb
from database import DB_CONFIG

def check_joins():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("\nCHECKING JOINS FOR ENT 1:")
    
    # Check User Join
    cursor.execute("""
        SELECT p.id, p.usuario_id, u.id, u.enterprise_id
        FROM prestamos p
        LEFT JOIN usuarios u ON p.usuario_id = u.id
        WHERE p.enterprise_id = 1
    """)
    for r in cursor.fetchall():
        match = "MATCH" if r[2] and r[3] == 1 else "FAIL"
        print(f"Loan {r[0]} User {r[1]}: UserFound={r[2]}, UserEnt={r[3]} -> {match}")
        
    # Check Book Join
    cursor.execute("""
        SELECT p.id, p.libro_id, l.id, l.enterprise_id
        FROM prestamos p
        LEFT JOIN libros l ON p.libro_id = l.id
        WHERE p.enterprise_id = 1
    """)
    for r in cursor.fetchall():
        match = "MATCH" if r[2] and r[3] == 1 else "FAIL"
        print(f"Loan {r[0]} Book {r[1]}: BookFound={r[2]}, BookEnt={r[3]} -> {match}")

    conn.close()

if __name__ == "__main__":
    check_joins()
