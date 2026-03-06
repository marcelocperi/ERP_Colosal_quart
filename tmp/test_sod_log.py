
from database import get_db_cursor

def test_enriquecimiento_log():
    # Simulamos el guardado de un rol con conflicto
    # Role 62 es AUDITOR
    # Buscamos IDs de permisos que chocan
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, code, description FROM sys_permissions WHERE code IN ('create_pago', 'create_orden_compra', 'view_articulos')")
        perms_data = cursor.fetchall()
        
        perm_ids = [str(p['id']) for p in perms_data]
        role_id = 62
        
        # --- LOGICA SIMULADA DE ROUTES.PY ---
        cursor.execute("SELECT name FROM sys_roles WHERE id = %s", (role_id,))
        role_name = cursor.fetchone()['name']
        
        from services.sod_service import analyze_role_sod
        sod_analysis = analyze_role_sod(role_name, perms_data)
        
        violaciones_str = ", ".join([v['regla'] for v in sod_analysis['conflictos_detalle']]) if sod_analysis['conflictos_detalle'] else "Ninguna"
        inocuos_count = len(sod_analysis['inocuos'])
        
        details = f"TEST PRUEBA ÁCIDA: Rol: {role_name} (ID:{role_id}). Violaciones: {violaciones_str}. Inocuos: {inocuos_count}. Total: {len(perm_ids)}."
        
        # Insertar en log (Simulando lo que pasaría al darle 'Guardar' en la UI)
        cursor.execute("""
            INSERT INTO sys_security_logs 
            (enterprise_id, actor_user_id, action, status, details, ip_address, session_id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (0, 9, 'UPDATE_ROLE_PERMS', 'SUCCESS', details, '127.0.0.1', 'SESSION_TEST_CISA'))
        
        print(f"Log de seguridad generado con éxito:")
        print(f"DETALLE REGISTRADO: {details}")

if __name__ == "__main__":
    test_enriquecimiento_log()
