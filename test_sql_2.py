from database import get_db_cursor
import traceback

def test():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            ent_id = 1
            print(f"Testing with ent_id={ent_id}")
            # The ORIGINAL query (before my latest changes)
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
                HAVING COALESCE(SUM(e.cantidad), 0) <= a.punto_pedido AND a.punto_pedido > 0
                ORDER BY (a.punto_pedido - COALESCE(SUM(e.cantidad), 0)) DESC
            """
            cursor.execute(sql, (ent_id,))
            rows = cursor.fetchall()
            print(f"Success! {len(rows)} rows.")
    except Exception:
        print(traceback.format_exc())

if __name__ == "__main__":
    test()
