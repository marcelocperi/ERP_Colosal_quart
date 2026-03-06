
from database import get_db_cursor
from datetime import datetime

def update_replacement_costs():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Get all articles
            cursor.execute("SELECT id, enterprise_id, costo FROM stk_articulos")
            articulos = cursor.fetchall()
            
            total_updated = 0
            for art in articulos:
                art_id = art['id']
                ent_id = art['enterprise_id']
                master_cost = art['costo'] or 0
                
                # Find last purchase
                # modulo='COMPRAS' and we want the latest fecha_emision
                query = """
                    SELECT d.precio_unitario, c.fecha_emision
                    FROM erp_comprobantes_detalle d
                    JOIN erp_comprobantes c ON d.comprobante_id = c.id
                    WHERE d.articulo_id = %s 
                      AND c.enterprise_id = %s
                      AND c.modulo = 'COMPRAS'
                    ORDER BY c.fecha_emision DESC, c.id DESC
                    LIMIT 1
                """
                cursor.execute(query, (art_id, ent_id))
                last_purchase = cursor.fetchone()
                
                if last_purchase:
                    new_cost = last_purchase['precio_unitario']
                    new_date = last_purchase['fecha_emision']
                else:
                    new_cost = master_cost
                    new_date = None
                
                # Update article
                cursor.execute("""
                    UPDATE stk_articulos 
                    SET costo_reposicion = %s, fecha_costo_reposicion = %s
                    WHERE id = %s
                """, (new_cost, new_date, art_id))
                total_updated += 1
                
            print(f"Updated {total_updated} articles.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_replacement_costs()
