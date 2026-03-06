from database import get_db_cursor
from werkzeug.security import generate_password_hash, check_password_hash
import sys

def test_login(username, ent_id, password):
    print(f"Testing login for {username} in enterprise {ent_id}...")
    with get_db_cursor() as cursor:
        # 1. Check Enterprise
        cursor.execute("SELECT id, nombre, estado, is_saas_owner FROM sys_enterprises WHERE id = ?", (ent_id,))
        ent = cursor.fetchone()
        if not ent:
            print(f"FAILED: Enterprise {ent_id} not found")
            return
        print(f"Enterprise found: {ent}")

        # 2. Check User
        cursor.execute("SELECT id, password_hash FROM sys_users WHERE username = ? AND enterprise_id = ?", (username, ent_id))
        user = cursor.fetchone()
        if not user:
            print(f"FAILED: User {username} not found in enterprise {ent_id}")
            # Let's list users in that enterprise
            cursor.execute("SELECT username FROM sys_users WHERE enterprise_id = ?", (ent_id,))
            users = cursor.fetchall()
            print(f"Users in ent {ent_id}: {users}")
            return
        
        user_id, pwd_hash = user
        print(f"User found: ID={user_id}")
        
        # 3. Check Password
        if check_password_hash(pwd_hash, password):
            print("SUCCESS: Password hash matches!")
        else:
            print("FAILED: Password hash MISMATCH")
            # Let's re-generate and check
            new_h = generate_password_hash(password)
            print(f"Current hash in DB: {pwd_hash}")
            print(f"Newly generated hash: {new_h}")
            if check_password_hash(new_h, password):
                 print("Re-check of newly generated hash: SUCCESS (Hash logic is ok)")
            else:
                 print("Re-check of newly generated hash: FAILED (Something is very wrong with hash logic)")

if __name__ == "__main__":
    test_login('superadmin', 0, 'Taz100')
