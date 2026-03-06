import sys
import argparse

sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
from database import get_db_cursor

try:
    with get_db_cursor(commit=True) as cursor:
        print("Consultando base de datos pre-existente...")
        cursor.execute("SELECT id FROM sys_permissions WHERE code = 'admin_articulos'")
        if not cursor.fetchone():
            print("El permiso 'admin_articulos' fue detectado como FALTANTE.")
            # Inyectamos el permiso globalmente (enterprise_id = 0 para toda instalación SaaS)
            cursor.execute("""
                INSERT INTO sys_permissions (code, description, category, enterprise_id) 
                VALUES ('admin_articulos', 'Alta, Baja, Modificación e Importación de artículos maestros', 'STOCK', 0)
            """)
            print("🟢 ¡Éxito! Permiso 'admin_articulos' inyectado de forma robusta en la tabla sys_permissions.")
            
            # Autocorrección adicional SoD:
            # Asignemos este permiso crítico de importación al rol "CONFIGURADOR" 
            # y "ALMACENISTA" (o un nuevo rol) por defecto como punto de partida.
            # Veamos si existen los roles globalmente o en empresa 1
            print("Revisando roles para inyectar permiso por defecto (ALMACENISTA)...")
            cursor.execute("SELECT id, enterprise_id FROM sys_roles WHERE name = 'ALMACENISTA'")
            almacenes = cursor.fetchall()
            
            # Get internal ID of the permission we just created
            cursor.execute("SELECT id FROM sys_permissions WHERE code = 'admin_articulos'")
            perm_id_row = cursor.fetchone()
            if perm_id_row:
                 perm_id = perm_id_row[0]
                 for al in almacenes:
                     role_id = al[0]
                     ent_id = al[1]
                     # Check if it already has it
                     cursor.execute("SELECT id FROM sys_role_permissions WHERE role_id=%s AND permission_id=%s", (role_id, perm_id))
                     if not cursor.fetchone():
                          cursor.execute("INSERT INTO sys_role_permissions (enterprise_id, role_id, permission_id) VALUES (%s, %s, %s)", (ent_id, role_id, perm_id))
                 print("🟢 Permiso asignado por defecto a todos los roles 'ALMACENISTA' locales existentes.")
        else:
            print("🟡 El permiso ya figuraba inscripto en la base de datos.")
            
except Exception as e:
    print(f"❌ Error inyectando el permiso: {e}")
