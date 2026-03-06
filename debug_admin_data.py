
import mariadb
import sys

# DB CONFIG
DB_CONFIG = {
    'user': 'root',
    'password': '123',
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'biblioteca_db'
}

def get_db_connection():
    try:
        conn = mariadb.connect(**DB_CONFIG)
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        sys.exit(1)

def debug_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    ENTERPRISE_ID = 1
    USER_ID = 1 # Admin

    print(f"--- DEBUGGING DATA FOR ENTERPRISE {ENTERPRISE_ID} (User {USER_ID}) ---")

    # 1. Check Permissions
    print("\n[1] Checking Permissions for User 1...")
    cursor.execute("""
        SELECT p.code 
        FROM roles_permissions rp
        JOIN permissions p ON rp.permission_id = p.id
        JOIN users_roles ur ON rp.role_id = ur.role_id
        WHERE ur.user_id = ? AND ur.enterprise_id = ?
    """, (USER_ID, ENTERPRISE_ID))
    perms = [row[0] for row in cursor.fetchall()]
    print(f"   Explicit Permissions: {perms}")
    
    # 2. Check Users Data
    print("\n[2] Checking 'usuarios' table for Enterprise 1...")
    cursor.execute("SELECT COUNT(*) FROM usuarios WHERE enterprise_id = ?", (ENTERPRISE_ID,))
    count_users = cursor.fetchone()[0]
    print(f"   Total Users found in DB: {count_users}")
    
    if count_users > 0:
        cursor.execute("SELECT id, username, nombre, apellido, role_id FROM usuarios WHERE enterprise_id = ? LIMIT 5", (ENTERPRISE_ID,))
        print("   Sample Users:")
        for u in cursor.fetchall():
            print(f"   - ID: {u[0]}, User: {u[1]}, Name: {u[2]} {u[3]}, RoleID: {u[4]}")
    else:
        print("   !! WARNING: No users found for this enterprise.")

    # 3. Check Loans Data
    print("\n[3] Checking 'prestamos' table for Enterprise 1...")
    cursor.execute("SELECT COUNT(*) FROM prestamos WHERE enterprise_id = ? AND fecha_devolucion_real IS NULL", (ENTERPRISE_ID,))
    count_loans = cursor.fetchone()[0]
    print(f"   Active Loans found in DB: {count_loans}")

    if count_loans > 0:
        cursor.execute("""
            SELECT p.id, u.nombre, l.nombre, p.fecha_prestamo
            FROM prestamos p
            JOIN usuarios u ON p.usuario_id = u.id
            JOIN libros l ON p.libro_id = l.id
            WHERE p.enterprise_id = ? AND p.fecha_devolucion_real IS NULL
            LIMIT 5
        """, (ENTERPRISE_ID,))
        print("   Sample Active Loans:")
        for p in cursor.fetchall():
            print(f"   - Loan #{p[0]}: User {p[1]} -> Book {p[2]} (Date: {p[3]})")
    else:
        print("   !! WARNING: No active loans found for this enterprise.")
        
        # Check all loans (including returned)
        cursor.execute("SELECT COUNT(*) FROM prestamos WHERE enterprise_id = ?", (ENTERPRISE_ID,))
        total_loans = cursor.fetchone()[0]
        print(f"   Total Loans (History) found in DB: {total_loans}")


    conn.close()

if __name__ == "__main__":
    debug_data()
