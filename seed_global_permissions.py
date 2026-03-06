from database import get_db_cursor

def seed_global_permissions():
    permissions = [
        # Compras
        ('compras.dashboard', 'Acceso al tablero de compras', 'Compras'),
        ('compras.ver_reportes', 'Ver reportes de compras y alertas', 'Compras'),
        ('compras.gestionar_oc', 'Crear y gestionar órdenes de compra', 'Compras'),
        ('compras.proveedores', 'Gestión de maestros de proveedores', 'Compras'),
        
        # Fondos
        ('fondos.dashboard', 'Acceso al módulo de fondos', 'Fondos'),
        ('fondos.autorizar_pagos', 'Autorización de desembolsos (Tesorería)', 'Fondos'),
        ('fondos.gestionar_cajas', 'Gestión de cuentas y medios de pago', 'Fondos'),
        ('fondos.reportes', 'Reportes financieros y calendario de pagos', 'Fondos')
    ]
    
    with get_db_cursor() as cursor:
        print("Seeding global permissions (enterprise_id=0)...")
        for code, description, category in permissions:
            cursor.execute("SELECT id FROM sys_permissions WHERE code = %s AND enterprise_id = 0", (code,))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO sys_permissions (code, description, category, enterprise_id)
                    VALUES (%s, %s, %s, 0)
                """, (code, description, category))
                print(f"  + Added: {code}")
            else:
                print(f"  - Already exists: {code}")
                
    print("Permissions seeding finished.")

if __name__ == "__main__":
    seed_global_permissions()
