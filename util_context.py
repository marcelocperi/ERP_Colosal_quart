
import os
import sys

# Agregar el directorio actual al path para importar database
sys.path.append(os.getcwd())

from database import get_db_cursor

def get_system_context():
    print("=== CONTEXTO DEL SISTEMA ===")
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Empresas
            cursor.execute("SELECT id, nombre, cuit FROM sys_enterprises WHERE estado = 'activo'")
            enterprises = cursor.fetchall()
            print(f"\nEmpresas Activas ({len(enterprises)}):")
            for e in enterprises:
                print(f"  - ID: {e['id']} | {e['nombre']} (CUIT: {e['cuit']})")

            # 2. Usuarios Admin (para login de captura)
            cursor.execute("""
                SELECT u.username, u.enterprise_id, e.nombre as enterprise_name 
                FROM sys_users u 
                JOIN sys_enterprises e ON u.enterprise_id = e.id 
                WHERE u.username = 'admin' OR u.role_id IN (SELECT id FROM sys_roles WHERE name LIKE '%admin%')
                LIMIT 5
            """)
            users = cursor.fetchall()
            print("\nUsuarios Sugeridos para Pruebas/Capturas:")
            for u in users:
                print(f"  - Usuario: {u['username']} | Empresa: {u['enterprise_name']} (ID: {u['enterprise_id']})")

            # 3. Datos de Prueba (Ventas/Compras)
            cursor.execute("SELECT COUNT(*) as total FROM erp_comprobantes WHERE modulo = 'VENTAS'")
            v = cursor.fetchone()
            cursor.execute("SELECT COUNT(*) as total FROM erp_comprobantes WHERE modulo = 'COMPRAS'")
            c = cursor.fetchone()
            print(f"\nVolumen de datos: Ventas ({v['total']}) | Compras ({c['total']})")

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    get_system_context()
