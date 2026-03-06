"""
Crea el rol 'soporte_tecnico' en todas las enterprises y asigna al usuario admin.
Ejecutar una sola vez.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import get_db_cursor

ROL_NAME = 'soporte_tecnico'
ROL_DESC = 'Soporte Técnico — responsables de gestión de incidentes del sistema'

with get_db_cursor() as cursor:
    # 1. Obtener todas las enterprises activas
    cursor.execute("SELECT DISTINCT enterprise_id FROM sys_users ORDER BY enterprise_id")
    enterprises = [row[0] for row in cursor.fetchall()]
    print(f"Enterprises encontradas: {enterprises}")

    for eid in enterprises:
        # Crear rol si no existe
        cursor.execute(
            "SELECT id FROM sys_roles WHERE enterprise_id = %s AND name = %s",
            (eid, ROL_NAME)
        )
        existing = cursor.fetchone()
        if existing:
            role_id = existing[0]
            print(f"  [ent={eid}] Rol ya existe, id={role_id}")
        else:
            cursor.execute(
                "INSERT INTO sys_roles (enterprise_id, name, description) VALUES (%s, %s, %s)",
                (eid, ROL_NAME, ROL_DESC)
            )
            role_id = cursor.lastrowid
            print(f"  [ent={eid}] Rol creado, id={role_id}")

        # Asignar al usuario admin de esa enterprise
        cursor.execute(
            "SELECT id, username FROM sys_users WHERE enterprise_id = %s AND (username = 'admin' OR username = 'superadmin') ORDER BY id LIMIT 1",
            (eid,)
        )
        admin_user = cursor.fetchone()
        if admin_user:
            cursor.execute(
                "UPDATE sys_users SET role_id = %s WHERE id = %s",
                (role_id, admin_user[0])
            )
            print(f"  [ent={eid}] Usuario '{admin_user[1]}' (id={admin_user[0]}) asignado al rol soporte_tecnico")
        else:
            print(f"  [ent={eid}] No se encontró usuario admin")

print("\nListo. Rol 'soporte_tecnico' creado y admin asignado.")
