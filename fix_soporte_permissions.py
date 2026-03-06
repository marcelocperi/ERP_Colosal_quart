import sys
from database import get_db_cursor

PERMISSIONS_TO_ADD = [
    'view_error_log',
    'view_risk_dashboard',
    'manage_mitigation_rules',
    'view_mitigation_history',
    'dashboard_view',
    'sysadmin'  # Or just the specific ones?
]

def map_soporte_tecnico():
    # Only assign the granular ones to follow the principle of least privilege, unless sysadmin is specifically needed.
    # Actually, sysadmin gives them global enterprise control, maybe we just want the error log one.
    granular_perms = [
        'view_error_log',
        'view_risk_dashboard',
        'manage_mitigation_rules',
        'view_mitigation_history',
        'dashboard_view'
    ]

    with get_db_cursor(dictionary=True) as cursor:
        # Get all records of soporte_tecnico
        cursor.execute("SELECT id, enterprise_id FROM sys_roles WHERE name = 'soporte_tecnico'")
        roles = cursor.fetchall()
        
        if not roles:
            print("Role 'soporte_tecnico' does not exist in the database.")
            return

        # Find permission IDs for the granular perms
        cursor.execute(f"SELECT id, code, enterprise_id FROM sys_permissions WHERE code IN ({','.join(['%s']*len(granular_perms))}) AND enterprise_id=0", tuple(granular_perms))
        perms = {p['code']: p['id'] for p in cursor.fetchall()}

        for role in roles:
            role_id = role['id']
            ent_id = role['enterprise_id']
            added = 0
            for code in granular_perms:
                perm_id = perms.get(code)
                if not perm_id:
                    print(f"Warning: Permission {code} not found in sys_permissions.")
                    continue
                
                # Check if it's already assigned
                cursor.execute("SELECT * FROM sys_role_permissions WHERE role_id = %s AND permission_id = %s", (role_id, perm_id))
                if not cursor.fetchone():
                    cursor.execute(
                        "INSERT INTO sys_role_permissions (role_id, permission_id, enterprise_id) VALUES (%s, %s, %s)",
                        (role_id, perm_id, ent_id)
                    )
                    added += 1
            print(f"Assigned {added} permissions to 'soporte_tecnico' (enterprise_id={ent_id})")

if __name__ == "__main__":
    map_soporte_tecnico()
