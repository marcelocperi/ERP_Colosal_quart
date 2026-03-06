# -*- coding: utf-8 -*-
"""
Setup Módulo de Compras & Inventario Crítico
===========================================
1. Asegura estructura en stk_articulos (stock_minimo).
2. Crea tablas de Compras (Ordenes y Detalles).
3. Genera datos de prueba de 'Inventario Crítico' (stock bajo vs minimo).
"""
import sys, os, io, random

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'multiMCP'))
from database import get_db_cursor

def init_purchasing_schema(c):
    print("--- Inicializando Tablas de Compras ---")
    
    # Asegurar stock_minimo en articulos
    c.execute("DESC stk_articulos")
    cols = [r['Field'] for r in c.fetchall()]
    if 'stock_minimo' not in cols:
        print("   + Agregando 'stock_minimo' a stk_articulos...")
        c.execute("ALTER TABLE stk_articulos ADD COLUMN stock_minimo INT DEFAULT 0 AFTER descripcion")
    if 'punto_pedido' not in cols:
        print("   + Agregando 'punto_pedido' a stk_articulos...")
        c.execute("ALTER TABLE stk_articulos ADD COLUMN punto_pedido INT DEFAULT 0 AFTER stock_minimo")

    # Tabla Encabezado Orden Compra
    c.execute("""
        CREATE TABLE IF NOT EXISTS cmp_ordenes_compra (
            id INT PRIMARY KEY AUTO_INCREMENT,
            enterprise_id INT DEFAULT 0,
            proveedor_id INT NOT NULL,
            fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
            estado ENUM('BORRADOR', 'CONFIRMADA', 'ENVIADA', 'RECIBIDA_PARCIAL', 'RECIBIDA_TOTAL', 'CANCELADA') DEFAULT 'BORRADOR',
            observaciones TEXT,
            total DECIMAL(10,2) DEFAULT 0.00,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (proveedor_id) REFERENCES proveedores(id),
            INDEX idx_ent (enterprise_id),
            INDEX idx_estado (estado)
        )
    """)
    print("   + Tabla 'cmp_ordenes_compra' verificada.")

    # Tabla Detalle Orden Compra
    c.execute("""
        CREATE TABLE IF NOT EXISTS cmp_detalles_orden (
            id INT PRIMARY KEY AUTO_INCREMENT,
            orden_id INT NOT NULL,
            articulo_id INT NOT NULL,
            cantidad_solicitada INT NOT NULL,
            cantidad_recibida INT DEFAULT 0,
            precio_unitario DECIMAL(10,2) DEFAULT 0.00,
            subtotal DECIMAL(10,2) DEFAULT 0.00,
            FOREIGN KEY (orden_id) REFERENCES cmp_ordenes_compra(id),
            FOREIGN KEY (articulo_id) REFERENCES stk_articulos(id),
            INDEX idx_orden (orden_id)
        )
    """)
    print("   + Tabla 'cmp_detalles_orden' verificada.")

def generar_necesidad_stock(c):
    print("\n--- Generando Escenario de Inventario Crítico ---")
    
    # Seleccionar algunos articulos al azar para ponerles stock minimo alto
    c.execute("SELECT id, nombre FROM stk_articulos WHERE enterprise_id=0 LIMIT 20")
    articulos = c.fetchall()
    
    if not articulos:
        print("   ! No hay articulos en enterprise_id=0 para simular inventario critico.")
        return

    # Reset stock minimo
    c.execute("UPDATE stk_articulos SET stock_minimo=0, punto_pedido=0 WHERE enterprise_id=0")
    
    criticos = random.sample(articulos, min(len(articulos), 10))
    for art in criticos:
        stock_min = random.choice([10, 20, 50, 100])
        # Asumimos que stock actual es 0 o bajo (no tenemos tabla de stock fisico aqui, 
        # pero asumiremos que el reporte compara contra inventario teorico o 0 si no hay movs)
        
        # Seteamos stock minimo
        c.execute("UPDATE stk_articulos SET stock_minimo=%s, punto_pedido=%s WHERE id=%s", 
                  (stock_min, stock_min + 5, art['id']))
        print(f"   -> Articulo '{art['nombre'][:30]}...' ahora requiere min {stock_min} un.")

def reporte_inventario_critico(c):
    print("\n--- REPORTE: TOMA DE INVENTARIO CRITICO ---")
    # Query compleja: Articulos donde (Stock Actual < Stock Minimo)
    # Como no tenemos tabla maestra de stock consolidado facil, haremos un left join a movs o asumimos 0
    # OJO: La tabla stock_movimientos es compleja. 
    # Para simplificar este MVP, asumiremos stock=0 si no calculamos saldo.
    
    # Vamos a usar una vista simplificada o subquery
    query = """
        SELECT 
            a.id, a.codigo, a.nombre, 
            a.stock_minimo,
            IFNULL(sum(m.cantidad), 0) as stock_actual,
            (a.stock_minimo - IFNULL(sum(m.cantidad), 0)) as faltante
        FROM stk_articulos a
        LEFT JOIN stk_movimientos m ON a.id = m.articulo_id
        WHERE a.enterprise_id=0 AND a.stock_minimo > 0
        GROUP BY a.id
        HAVING stock_actual < a.stock_minimo
        ORDER BY faltante DESC
    """
    # Nota: Si stk_movimientos no existe o tiene otra estructura, esto fallará. 
    # Verifiquemos si existe stk_movimientos.
    try:
        c.execute(query)
        resultados = c.fetchall()
        
        if not resultados:
            print("   Todo el stock está OK (o no hay movimientos registrados).")
        else:
            print(f"   DETECTADOS {len(resultados)} ARTICULOS CRITICOS:")
            print(f"   {'CODIGO':<15} {'NOMBRE':<30} {'MINIMO':<10} {'ACTUAL':<10} {'FALTANTE'}")
            print("-" * 80)
            for r in resultados:
                print(f"   {r['codigo']:<15} {r['nombre'][:28]:<30} {r['stock_minimo']:<10} {int(r['stock_actual']):<10} {int(r['faltante'])}")
                
            return resultados
    except Exception as e:
        print(f"   ! No se pudo calcular stock actual (falta tabla movimientos?): {e}")
        # Fallback: Asumir stock 0 para demostración
        c.execute("SELECT id, codigo, nombre, stock_minimo, 0 as stock_actual, stock_minimo as faltante FROM stk_articulos WHERE enterprise_id=0 AND stock_minimo > 0")
        return c.fetchall()

def generar_orden_automatica(c, criticos):
    if not criticos: return
    
    print("\n--- Generando Orden de Compra Automática ---")
    
    # Agrupar faltantes por "Rubro" -> Proveedor?
    # Como no tenemos link directo Articulo->Proveedor (aun), asignamos uno random del rubro (si existiera)
    # O simplemente un proveedor "General" o random.
    
    c.execute("SELECT id, razon_social FROM proveedores WHERE enterprise_id=0 LIMIT 5")
    proveedores = c.fetchall()
    
    if not proveedores:
        print("   ! No hay proveedores para generar orden.")
        return

    prov = random.choice(proveedores)
    print(f"   Generando OC para proveedor: {prov['razon_social']}")
    
    # Crear Cabecera
    c.execute("""
        INSERT INTO cmp_ordenes_compra (enterprise_id, proveedor_id, estado, observaciones)
        VALUES (0, %s, 'BORRADOR', 'Generada automaticamente por Inventario Critico')
    """, (prov['id'],))
    orden_id = c.lastrowid
    
    total = 0
    items = 0
    for art in criticos:
        if isinstance(art, dict): # DictCursor vs Tuple
            aid = art['id']
            cant = art['faltante']
        else:
            aid = art[0]
            cant = art[5]
            
        precio = random.randint(100, 5000) # Precio simulado
        
        c.execute("""
            INSERT INTO cmp_detalles_orden (orden_id, articulo_id, cantidad_solicitada, precio_unitario, subtotal)
            VALUES (%s, %s, %s, %s, %s)
        """, (orden_id, aid, cant, precio, cant * precio))
        total += (cant * precio)
        items += 1
        
    # Actualizar total
    c.execute("UPDATE cmp_ordenes_compra SET total=%s WHERE id=%s", (total, orden_id))
    print(f"   OC #{orden_id} creada exitosamente con {items} items. Total estimado: ${total:,.2f}")


def main():
    try:
        with get_db_cursor(dictionary=True) as c:
            init_purchasing_schema(c)
            generar_necesidad_stock(c)
            criticos = report_inventario_critico(c) if 'report_inventario_critico' in globals() else reporte_inventario_critico(c)
            if criticos:
                generar_orden_automatica(c, criticos)
            
        print("\n=== MÓDULO DE COMPRAS INICIALIZADO ===")
    except Exception as e:
        print(f"CRASH: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
