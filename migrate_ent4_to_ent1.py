
import mariadb
from database import DB_CONFIG

def migrate():
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    tables = [
        'libros', 'usuarios', 'prestamos', 'stock_motivos', 
        'stock_ajustes', 'movimientos_pendientes', 'cotizacion_dolar', 
        'sys_roles' 
        # sys_users we might want to be careful. Admin is in Ent 1. 
        # If there are users in Ent 4 (like 'admin' of Ent 4), we might handle them.
    ]
    
    print("Migrating data from Enterprise 4 to Enterprise 1...")
    
    for t in tables:
        try:
            cursor.execute(f"UPDATE {t} SET enterprise_id = 1 WHERE enterprise_id = 4")
            print(f"Updated {cursor.rowcount} rows in table '{t}'")
        except Exception as e:
            print(f"Error updating {t}: {e}")
            
    # Also update sys_users, but be careful not to conflict usernames if any
    # Since we saw a user 'admin' in Ent 4 in previous dumps (maybe?), let's check.
    # Actually, sys_users has a unique constraint on username usually? 
    # If Ent 4 has an 'admin' and Ent 1 has 'admin', we can't merge them easily.
    
    try:
        cursor.execute("UPDATE sys_users SET enterprise_id = 1 WHERE enterprise_id = 4 AND username != 'admin'")
        print(f"Updated {cursor.rowcount} rows in sys_users (excluding 'admin')")
    except Exception as e:
        print(f"Error updating sys_users: {e}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    migrate()
