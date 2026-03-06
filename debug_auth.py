from database import get_db_cursor

# Simulate the auth flow for user 8 in enterprise 4
user_id = 8
ent_id = 4

with get_db_cursor() as cursor:
    # Step 1: Get user info
    cursor.execute("""
        SELECT u.id, u.username, r.name as role_name 
        FROM sys_users u 
        LEFT JOIN sys_roles r ON u.role_id = r.id AND r.enterprise_id = u.enterprise_id
        WHERE u.id = ? AND u.enterprise_id = ?
    """, (user_id, ent_id))
    user_row = cursor.fetchone()
    
    if user_row:
        u_id, u_name, r_name = user_row
        username_clean = str(u_name).strip().lower()
        role_clean = (r_name or 'Sin Rol').strip()
        
        print(f"User ID: {u_id}")
        print(f"Username: {u_name}")
        print(f"Username Clean: {username_clean}")
        print(f"Role Name: {r_name}")
        print(f"Role Clean: {role_clean}")
        print(f"Role Clean Lower: {role_clean.lower()}")
        
        # Step 2: Load permissions
        cursor.execute("""
            SELECT DISTINCT p.code 
            FROM sys_permissions p
            JOIN sys_role_permissions rp ON p.id = rp.permission_id
            JOIN sys_users u ON u.role_id = rp.role_id AND u.enterprise_id = rp.enterprise_id
            WHERE u.id = ? AND u.enterprise_id = ?
        """, (user_id, ent_id))
        permissions = [str(row[0]).lower().strip() for row in cursor.fetchall()]
        
        print(f"\nBase Permissions: {permissions}")
        
        # Step 3: Check Admin Bypass
        try: 
            is_id_1 = int(u_id) == 1
        except: 
            is_id_1 = False
        
        role_lower = role_clean.lower()
        is_admin_role = role_lower in ['admin', 'administrador', 'administrator']
            
        print(f"\nAdmin Bypass Checks:")
        print(f"  username_clean == 'admin': {username_clean == 'admin'}")
        print(f"  role_lower: '{role_lower}'")
        print(f"  is_admin_role: {is_admin_role}")
        print(f"  is_id_1: {is_id_1}")
        
        if username_clean == 'admin' or is_admin_role or is_id_1:
            if 'all' not in permissions: 
                permissions.append('all')
                print("  -> 'all' permission ADDED")
        
        print(f"\nFinal Permissions: {permissions}")
