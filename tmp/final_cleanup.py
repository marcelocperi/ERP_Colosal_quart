
from database import get_db_cursor

def cleanup():
    with get_db_cursor() as cursor:
        # 1. Eliminar permisos de conflicto
        cursor.execute("""
            DELETE FROM sys_role_permissions 
            WHERE role_id = 62 
            AND permission_id IN (SELECT id FROM sys_permissions WHERE code IN ('create_pago', 'create_orden_compra'))
        """)
        print("Restauración técnica: Permisos abusivos removidos del rol AUDITOR.")

        # 2. Consultar logs para reporte final
        cursor.execute("""
            SELECT event_time, action, details 
            FROM sys_security_logs 
            WHERE action LIKE '%PERMISSION%' OR action LIKE '%ROLE%'
            ORDER BY id DESC LIMIT 3
        """)
        logs = cursor.fetchall()
        print("\nRASTRO DE AUDITORÍA DETECTADO:")
        for log in logs:
            print(f"- {log[0]} | {log[1]} | {log[2][:100]}...")

if __name__ == "__main__":
    cleanup()
