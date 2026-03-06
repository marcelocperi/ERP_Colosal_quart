from database import get_db_cursor
import os

def test():
    ent_id = 0 # Default user usually has enterprise 0 or 1
    # Try to find a valid ent_id
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id FROM erp_empresas LIMIT 1")
        row = cursor.fetchone()
        if row: ent_id = row['id']
        
        print(f"Testing with enterprise_id: {ent_id}")
        
        sql = """
            SELECT 
                a.id, a.codigo, a.nombre, a.punto_pedido, a.stock_minimo,
                COALESCE(SUM(e.cantidad), 0) as stock_actual,
                (a.stock_minimo - COALESCE(SUM(e.cantidad), 0)) as sugerido,
                ap.proveedor_id, t.nombre as proveedor_nombre, ap.lead_time_dias,
                ap.es_habitual, t.es_proveedor_extranjero
            FROM stk_articulos a
            LEFT JOIN stk_existencias e ON a.id = e.articulo_id AND a.enterprise_id = e.enterprise_id
            LEFT JOIN cmp_articulos_proveedores ap ON a.id = ap.articulo_id AND a.enterprise_id = ap.enterprise_id AND ap.es_habitual = 1
            LEFT JOIN erp_terceros t ON ap.proveedor_id = t.id
            WHERE a.enterprise_id = %s AND a.activo = 1
            GROUP BY a.id, a.codigo, a.nombre, a.punto_pedido, a.stock_minimo, ap.proveedor_id, t.nombre, ap.lead_time_dias, ap.es_habitual, t.es_proveedor_extranjero
            HAVING (SELECT COALESCE(SUM(cantidad), 0) FROM stk_existencias WHERE articulo_id = a.id AND enterprise_id = a.enterprise_id) <= a.punto_pedido 
               AND a.punto_pedido > 0
            ORDER BY (a.punto_pedido - (SELECT COALESCE(SUM(cantidad), 0) FROM stk_existencias WHERE articulo_id = a.id AND enterprise_id = a.enterprise_id)) DESC
        """
        try:
            cursor.execute(sql, (ent_id,))
            res = cursor.fetchall()
            print(f"Success! Found {len(res)} results.")
        except Exception as e:
            print(f"FAILED: {e}")

if __name__ == "__main__":
    test()
