import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def analyze_client_grants(cliente_id):
    with get_db_cursor(dictionary=True) as cursor:
        print(f"=== ANALYZING GRANTS FOR CLIENT ID {cliente_id} ===")
        
        # 1. Main Ficha
        cursor.execute("SELECT nombre, condicion_pago_id, condicion_mixta_id FROM erp_terceros WHERE id = %s", (cliente_id,))
        ficha = cursor.fetchone()
        print(f"Ficha: {ficha}")
        
        allowed_cond_ids = set()
        if ficha['condicion_pago_id']:
            allowed_cond_ids.add(ficha['condicion_pago_id'])
            
        # 2. Mixed Structure
        if ficha['condicion_mixta_id']:
            cursor.execute("SELECT condicion_pago_id FROM fin_condiciones_pago_mixtas_detalle WHERE mixta_id = %s", (ficha['condicion_mixta_id'],))
            for row in cursor.fetchall():
                allowed_cond_ids.add(row['condicion_pago_id'])
            print(f"Mixed Conditions IDs added: {allowed_cond_ids}")
            
        # 3. Specific Grants (erp_terceros_condiciones)
        cursor.execute("SELECT condicion_pago_id FROM erp_terceros_condiciones WHERE tercero_id = %s AND habilitado = 1", (cliente_id,))
        for row in cursor.fetchall():
            allowed_cond_ids.add(row['condicion_pago_id'])
            
        print(f"TOTAL Allowed Condition IDs: {allowed_cond_ids}")
        
        # 4. Resolve Names
        if allowed_cond_ids:
            ids_str = ",".join(map(str, allowed_cond_ids))
            cursor.execute(f"SELECT id, nombre FROM fin_condiciones_pago WHERE id IN ({ids_str})")
            print("Allowed Conditions Names:")
            for r in cursor.fetchall():
                print(f" - {r['nombre']} (ID: {r['id']})")

if __name__ == "__main__":
    analyze_client_grants(3) # La china Mari
