
import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor
from services.sourcing_service import SourcingService

def execute_phase_1_2():
    """
    Ejecuta la Fase 1.2: Reglas de Sourcing.
    Analiza el historial de facturas reales para poblar el maestro de proveedores por artículo.
    Prioriza al proveedor con la 'Última Factura' o 'Mejor Precio' como habitual.
    """
    try:
        with get_db_cursor(dictionary=True) as cursor:
            print("--- Ejecutando MSAC Fase 1.2: Reglas de Sourcing (Sync) ---")

            # 1. Obtener lista de Artículos con compras históricas
            sql_articles = """
                SELECT DISTINCT cd.articulo_id, c.enterprise_id
                FROM erp_comprobantes_detalle cd
                JOIN erp_comprobantes c ON cd.comprobante_id = c.id
                WHERE c.tipo_operacion = 'COMPRA'
            """
            cursor.execute(sql_articles)
            articles = cursor.fetchall()
            
            if not articles:
                print("No se encontraron compras históricas para analizar.")
                return

            print(f"Analizando sourcing para {len(articles)} artículos...")

            for art in articles:
                eid = art['enterprise_id']
                aid = art['articulo_id']
                
                # Obtener la mejor opción (Last Reception para mayor realismo de reposición)
                option = SourcingService.get_best_option(eid, aid, strategy='LAST_RECEPTION')
                
                if option:
                    prov_id = option['proveedor_id']
                    price = option['precio_referencia']
                    
                    # Verificar si ya existe en el maestro
                    cursor.execute("""
                        SELECT id FROM cmp_articulos_proveedores 
                        WHERE enterprise_id = %s AND articulo_id = %s AND proveedor_id = %s
                    """, (eid, aid, prov_id))
                    
                    row = cursor.fetchone()
                    if not row:
                        print(f"  > Agregando Proveedor {prov_id} para Articulo {aid} (Precio: {price})")
                        # Insertar con Origen Local (id=1 del seed anterior)
                        cursor.execute("""
                            INSERT INTO cmp_articulos_proveedores (enterprise_id, articulo_id, proveedor_id, origen_id, precio_referencia, es_habitual, user_id)
                            VALUES (%s, %s, %s, 1, %s, 1, 1)
                        """, (eid, aid, prov_id, price))
                    else:
                        print(f"  > Actualizando habitual para Articulo {aid}...")
                        cursor.execute("""
                            UPDATE cmp_articulos_proveedores SET es_habitual = 0 
                            WHERE enterprise_id = %s AND articulo_id = %s
                        """, (eid, aid))
                        cursor.execute("""
                            UPDATE cmp_articulos_proveedores SET es_habitual = 1, precio_referencia = %s
                            WHERE id = %s
                        """, (price, row['id']))

            print("SINCORNIZACIÓN DE REGLAS DE SOURCING COMPLETADA.")

    except Exception as e:
        print(f"Error en Fase 1.2: {e}")

if __name__ == "__main__":
    execute_phase_1_2()
