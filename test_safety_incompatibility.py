import os
import json
import sys
from database import get_db_cursor

def test_safety():
    print("Iniciando prueba de Incompatibilidad Química...")
    
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Buscar o crear artículos de prueba
        ent_id = 0
        print(f"Usando Enterprise ID: {ent_id}")
        
        # Articulo A: Ácido Sulfúrico (Corrosivo)
        cursor.execute("SELECT id FROM stk_articulos WHERE nombre = 'Acido Sulfurico 98%' AND enterprise_id = %s", (ent_id,))
        art_a = cursor.fetchone()
        if not art_a:
            cursor.execute("INSERT INTO stk_articulos (enterprise_id, nombre, precio_venta, tipo_articulo) VALUES (%s, 'Acido Sulfurico 98%', 1500, 'insumo')", (ent_id,))
            art_a_id = cursor.lastrowid
        else:
            art_a_id = art_a['id']
            
        # Articulo B: Alcohol Etílico (Inflamable)
        cursor.execute("SELECT id FROM stk_articulos WHERE nombre = 'Alcohol Etilico 96%' AND enterprise_id = %s", (ent_id,))
        art_b = cursor.fetchone()
        if not art_b:
            cursor.execute("INSERT INTO stk_articulos (enterprise_id, nombre, precio_venta, tipo_articulo) VALUES (%s, 'Alcohol Etilico 96%', 800, 'insumo')", (ent_id,))
            art_b_id = cursor.lastrowid
        else:
            art_b_id = art_b['id']
            
        # 2. Asignar Datos de Seguridad
        # Ácido: GHS05, Clase 8
        cursor.execute("""
            INSERT INTO stk_articulos_seguridad 
            (articulo_id, enterprise_id, numero_un, clase_riesgo, pictogramas_json) 
            VALUES (%s, %s, 'UN 1830', '8', '["GHS05"]')
            ON DUPLICATE KEY UPDATE clase_riesgo='8', pictogramas_json='["GHS05"]'
        """, (art_a_id, ent_id))
        
        # Alcohol: GHS02, Clase 3
        cursor.execute("""
            INSERT INTO stk_articulos_seguridad 
            (articulo_id, enterprise_id, numero_un, clase_riesgo, pictogramas_json) 
            VALUES (%s, %s, 'UN 1170', '3', '["GHS02"]')
            ON DUPLICATE KEY UPDATE clase_riesgo='3', pictogramas_json='["GHS02"]'
        """, (art_b_id, ent_id))
        
        # 3. Poner Ácido en Depósito 1
        cursor.execute("SELECT * FROM stk_depositos WHERE enterprise_id = %s LIMIT 1", (ent_id,))
        dep = cursor.fetchone()
        if not dep:
             print("No hay depósitos para probar.")
             return
        
        dep_id = dep['id']
        print(f"Usando Depósito: {dep['nombre']} (ID: {dep_id})")
        
        cursor.execute("""
            INSERT INTO stk_existencias (enterprise_id, deposito_id, articulo_id, cantidad)
            VALUES (%s, %s, %s, 100)
            ON DUPLICATE KEY UPDATE cantidad = 100
        """, (ent_id, dep_id, art_a_id))
        
        print(f"OK: Se ha estoqueado '{art_a_id}' (Acido) en Depósito {dep_id}")
        print("--- PRUEBA DE LÓGICA ---")
        
        # Ahora simulamos lo que haría la ruta al intentar meter Alcohol en el mismo depósito
        from core.safety_logic import get_incompatibility_alerts
        
        # Datos del entrante (Alcohol)
        cursor.execute("SELECT s.*, a.nombre as nombre_comun FROM stk_articulos_seguridad s JOIN stk_articulos a ON s.articulo_id = a.id WHERE s.articulo_id = %s", (art_b_id,))
        incoming = cursor.fetchone()
        incoming['pictogramas_json'] = json.loads(incoming['pictogramas_json'])
        
        # Datos de lo que ya hay
        cursor.execute("""
             SELECT s.*, a.nombre as nombre_comun 
             FROM stk_articulos_seguridad s
             JOIN stk_existencias e ON s.articulo_id = e.articulo_id
             JOIN stk_articulos a ON s.articulo_id = a.id
             WHERE e.deposito_id = %s AND e.cantidad > 0
        """, (dep_id,))
        existing = cursor.fetchall()
        for item in existing:
            item['pictogramas_json'] = json.loads(item['pictogramas_json'])
            
        alerts = get_incompatibility_alerts(incoming, existing)
        
        if alerts:
            print(f"ALERTA DETECTADA EXITOSAMENTE:")
            for a in alerts:
                print(f"[{a['severity']}] {a['message']}")
        else:
            print("ERROR: No se detectaron alertas de incompatibilidad.")

if __name__ == "__main__":
    test_safety()
