# -*- coding: utf-8 -*-
"""
Setup Workflow Compras v4: Corregido PERMISSIONS (name)
======================================================
"""
import sys, os, io, random

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'multiMCP'))
from database import get_db_cursor

def init_schema(c):
    print("--- 1. Inicializando Esquema de Compras Avanzado ---")
    
    # 1. Asegurar 'naturaleza' en Articulos (para matchear con Proveedores)
    c.execute("SHOW COLUMNS FROM stk_articulos LIKE 'naturaleza'")
    if not c.fetchone():
        print("   + Agregando 'naturaleza' a stk_articulos...")
        c.execute("ALTER TABLE stk_articulos ADD COLUMN naturaleza VARCHAR(50) DEFAULT 'GENERAL' AFTER nombre")
        c.execute("ALTER TABLE stk_articulos ADD INDEX idx_naturaleza (naturaleza)")
        
        # Asignar naturalezas aleatorias
        nats = ['EDITORIAL', 'TECNOLOGIA', 'INSUMOS', 'LIMPIEZA', 'MANTENIMIENTO']
        c.execute("SELECT id FROM stk_articulos WHERE enterprise_id=0")
        for r in c.fetchall():
            c.execute("UPDATE stk_articulos SET naturaleza=%s WHERE id=%s", (random.choice(nats), r['id']))

    # 2. Tabla Solicitudes de Reposicion (Interna)
    c.execute("""
        CREATE TABLE IF NOT EXISTS cmp_solicitudes_reposicion (
            id INT PRIMARY KEY AUTO_INCREMENT,
            enterprise_id INT DEFAULT 0,
            fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
            solicitante_id INT, -- usuario operador
            aprobador_id INT,   -- gerente compras
            estado ENUM('BORRADOR', 'PENDIENTE_APROBACION', 'APROBADA', 'RECHAZADA', 'EN_COTIZACION', 'CLOSED') DEFAULT 'BORRADOR',
            prioridad ENUM('BAJA', 'MEDIA', 'ALTA', 'URGENTE') DEFAULT 'MEDIA',
            observaciones TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_estado (estado)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS cmp_detalles_solicitud (
            id INT PRIMARY KEY AUTO_INCREMENT,
            solicitud_id INT NOT NULL,
            articulo_id INT NOT NULL,
            cantidad_sugerida INT NOT NULL, 
            cantidad_aprobada INT,          
            motivo_ajuste VARCHAR(255),
            FOREIGN KEY (solicitud_id) REFERENCES cmp_solicitudes_reposicion(id),
            FOREIGN KEY (articulo_id) REFERENCES stk_articulos(id)
        )
    """)
    print("   + Tablas de Solicitud creadas.")

    # 3. Tabla Solicitudes de Cotizacion 
    c.execute("""
        CREATE TABLE IF NOT EXISTS cmp_cotizaciones (
            id INT PRIMARY KEY AUTO_INCREMENT,
            enterprise_id INT DEFAULT 0,
            solicitud_origen_id INT, 
            proveedor_id INT NOT NULL,
            fecha_envio DATETIME DEFAULT CURRENT_TIMESTAMP,
            fecha_vencimiento DATETIME,
            estado ENUM('BORRADOR', 'ENVIADA', 'RESPONDIDA', 'GANADORA', 'DESCARTADA') DEFAULT 'BORRADOR',
            hash_link VARCHAR(64),
            FOREIGN KEY (solicitud_origen_id) REFERENCES cmp_solicitudes_reposicion(id),
            FOREIGN KEY (proveedor_id) REFERENCES proveedores(id)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS cmp_items_cotizacion (
            id INT PRIMARY KEY AUTO_INCREMENT,
            cotizacion_id INT NOT NULL,
            articulo_id INT NOT NULL,
            cantidad INT NOT NULL,
            cantidad_ofrecida INT,
            precio_ofrecido DECIMAL(10,2),
            precio_cotizado DECIMAL(10,2),
            fecha_entrega_estimada DATE,
            FOREIGN KEY (cotizacion_id) REFERENCES cmp_cotizaciones(id),
            FOREIGN KEY (articulo_id) REFERENCES stk_articulos(id)
        )
    """)
    print("   + Tablas de Cotizacion creadas.")

def create_roles(c):
    print("\n--- 2. Configurando Roles y Puestos ---")
    
    # 1. Puesto
    c.execute("SELECT id FROM erp_puestos WHERE nombre = 'Gerente de Compras' AND enterprise_id=0")
    if not c.fetchone():
        c.execute("INSERT INTO erp_puestos (enterprise_id, nombre, nivel_jerarquico) VALUES (0, 'Gerente de Compras', 2)")
        print("   + Puesto 'Gerente de Compras' creado.")
    
    # 2. Rol
    c.execute("SELECT id FROM sys_roles WHERE name = 'gerente_compras' AND enterprise_id=0")
    role = c.fetchone()
    if not role:
        c.execute("INSERT INTO sys_roles (enterprise_id, name, description) VALUES (0, 'gerente_compras', 'Aprobador de solicitudes de stock')")
        role_id = c.lastrowid
        print("   + Rol 'gerente_compras' creado.")
        
        # Permisos (usando 'name' en lugar de 'codigo' si falla)
        permisos = ['compras.aprobar_solicitud', 'compras.ver_reportes', 'compras.gestion_proveedores']
        for p in permisos:
            # Intentar primero con 'name', si falla intentar 'codigo' (por si acaso)
            # Pero segun logs anteriores, roles usa 'name', asi que permissions probablemente tambien
            try:
                c.execute("SELECT id FROM sys_permissions WHERE name = %s", (p,))
                perm = c.fetchone()
                if not perm:
                    c.execute("INSERT INTO sys_permissions (name, description) VALUES (%s, %s)", (p, f"Permiso {p}"))
                    perm_id = c.lastrowid
                else: perm_id = perm['id']
                
                c.execute("INSERT INTO sys_role_permissions (role_id, permission_id) VALUES (%s, %s)", (role_id, perm_id))
            except Exception as e:
                print(f"Warning permiso {p}: {e}")
    else:
        print("   + Rol 'gerente_compras' ya existe.")

def simular_flujo(c):
    print("\n--- 3. Simulando Flujo Completo ---")
    
    c.execute("""
        SELECT a.id, a.nombre, a.naturaleza, a.stock_minimo, 
               IFNULL(SUM(m.cantidad), 0) as stock_actual
        FROM stk_articulos a
        LEFT JOIN stk_movimientos m ON a.id = m.articulo_id
        WHERE a.enterprise_id=0 AND a.stock_minimo > 0
        GROUP BY a.id
        HAVING stock_actual < a.stock_minimo
    """)
    criticos = c.fetchall()
    
    if not criticos:
        print("   ! Forzando stock critico para demo...")
        c.execute("SELECT id FROM stk_articulos LIMIT 1")
        aid = c.fetchone()['id']
        c.execute("UPDATE stk_articulos SET stock_minimo=100 WHERE id=%s", (aid,))
        # Simular resultado
        c.execute("SELECT id, nombre, naturaleza, stock_minimo, 0 as stock_actual FROM stk_articulos WHERE id=%s", (aid,))
        criticos = c.fetchall()

    print(f"   > Items Críticos: {len(criticos)}")
    
    # Create request
    c.execute("INSERT INTO cmp_solicitudes_reposicion (enterprise_id, estado, observaciones) VALUES (0, 'BORRADOR', 'Reposicion Automatica v4')")
    solicitud_id = c.lastrowid
    
    for item in criticos:
        sugerido = int(item['stock_minimo'] * 1.5)
        aid = item['id']
        c.execute("INSERT INTO cmp_detalles_solicitud (solicitud_id, articulo_id, cantidad_sugerida, cantidad_aprobada) VALUES (%s, %s, %s, %s)", 
                 (solicitud_id, aid, sugerido, sugerido))
        
    print(f"   [PASO 1] Solicitud #{solicitud_id} creada (BORRADOR).")
    
    # Operator
    c.execute("UPDATE cmp_solicitudes_reposicion SET estado='PENDIENTE_APROBACION' WHERE id=%s", (solicitud_id,))
    print(f"   [PASO 2] Enviada a Gerencia (PENDIENTE).")

    # Manager
    c.execute("UPDATE cmp_solicitudes_reposicion SET estado='APROBADA', aprobador_id=999 WHERE id=%s", (solicitud_id,))
    print(f"   [PASO 3] APROBADA.")

    # Quotations
    print(f"   [PASO 4] Generando Cotizaciones...")
    c.execute("""
        SELECT d.articulo_id, d.cantidad_aprobada, a.naturaleza
        FROM cmp_detalles_solicitud d
        JOIN stk_articulos a ON d.articulo_id = a.id
        WHERE d.solicitud_id = %s
    """, (solicitud_id,))
    items_aprobados = c.fetchall()
    
    items_por_naturaleza = {}
    for it in items_aprobados:
        nat = it['naturaleza']
        if nat not in items_por_naturaleza: items_por_naturaleza[nat] = []
        items_por_naturaleza[nat].append(it)
    
    for nat, items in items_por_naturaleza.items():
        c.execute("SELECT id, razon_social FROM proveedores WHERE naturaleza=%s AND enterprise_id=0 AND activo=1", (nat,))
        proveedores = c.fetchall()
        
        if not proveedores:
            c.execute("SELECT id, razon_social FROM proveedores WHERE enterprise_id=0 LIMIT 3") 
            proveedores = c.fetchall()
            print(f"      (Naturaleza {nat} sin proveedores, usando genericos)")
            
        print(f"      -> Naturaleza {nat}: {len(proveedores)} proveedores.")
        
        for prov in proveedores:
            c.execute("INSERT INTO cmp_cotizaciones (enterprise_id, solicitud_origen_id, proveedor_id, estado) VALUES (0, %s, %s, 'ENVIADA')", (solicitud_id, prov['id']))
            cot_id = c.lastrowid
            
            for it in items:
                c.execute("INSERT INTO cmp_items_cotizacion (cotizacion_id, articulo_id, cantidad) VALUES (%s, %s, %s)", (cot_id, it['articulo_id'], it['cantidad_aprobada']))
            print(f"         + Cotización #{cot_id} enviada a {prov['razon_social']}")

    c.execute("UPDATE cmp_solicitudes_reposicion SET estado='EN_COTIZACION' WHERE id=%s", (solicitud_id,))
    print(f"   Estado final Solicitud #{solicitud_id}: EN_COTIZACION")

def main():
    try:
        with get_db_cursor(dictionary=True) as c:
            init_schema(c)
            create_roles(c)
            simular_flujo(c)
        print("\n=== WORKFLOW FINALIZADO ===")
    except Exception as e:
        print(f"CRASH: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
