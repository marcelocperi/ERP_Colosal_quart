
from database import get_db_cursor
import datetime

def acid_test():
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Buscar el rol superadmin para ver su ID (está en enterprise_id 0)
        cursor.execute("SELECT id, role_id FROM sys_users WHERE username = 'superadmin' AND enterprise_id = 0")
        user = cursor.fetchone()
        if not user:
            print("Error: superadmin no encontrado")
            return
        
        user_id = user['id']
        role_id = user['role_id']
        
        # 2. Inyectar permisos de Conflicto al rol actual de superadmin (AUDITOR)
        cursor.execute("SELECT id FROM sys_permissions WHERE code IN ('create_pago', 'create_orden_compra')")
        perms = cursor.fetchall()
        for p in perms:
            try:
                cursor.execute("INSERT INTO sys_role_permissions (enterprise_id, role_id, permission_id) VALUES (1, %s, %s)", (role_id, p['id']))
            except: pass
        
        print(f"Conflicto SoD inyectado en el rol del operador (ID {role_id})")

        # 3. Crear transacción de prueba: Orden de Pago #99999
        cursor.execute("SELECT id FROM proveedores LIMIT 1")
        prov = cursor.fetchone()
        prov_id = prov['id'] if prov else 1
        
        ahora = datetime.datetime.now()
        cursor.execute("""
            INSERT INTO fin_ordenes_pago (enterprise_id, numero, fecha, tercero_id, importe_total, user_id, estado)
            VALUES (1, 99999, %s, %s, 155000.00, %s, 'EMITIDA')
        """, (ahora, prov_id, user_id))
        
        # 4. Crear transacción de prueba: Movimiento de Stock #88888
        cursor.execute("INSERT INTO stk_movimientos (enterprise_id, fecha, motivo_id, user_id, observaciones, estado) VALUES (1, %s, 31, %s, 'Prueba Ácida Auditoría Integ.', 'CONFIRMADO')", (ahora, user_id))

        print(f"Transacciones de prueba generadas para auditoría (OP #99999 y Movimiento Stock).")
        print("Vaya al módulo 'Integridad Transaccional' para ver los resultados.")

if __name__ == "__main__":
    acid_test()
