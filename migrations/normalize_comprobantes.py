import sys
import os
sys.path.append(os.getcwd())
from database import get_db_cursor

def normalize():
    with get_db_cursor(dictionary=True) as cursor:
        # 1. Get Enterprise CUITs
        cursor.execute("SELECT id, cuit FROM sys_enterprises")
        enterprises = {row['id']: row['cuit'] for row in cursor.fetchall()}

        # 2. Update existing rows
        cursor.execute("""
            SELECT c.id, c.enterprise_id, c.modulo, t.cuit as tercero_cuit 
            FROM erp_comprobantes c
            JOIN erp_terceros t ON c.tercero_id = t.id
        """)
        rows = cursor.fetchall()

        for row in rows:
            comp_id = row['id']
            ent_id = row['enterprise_id']
            modulo = row['modulo']
            tercero_cuit = row['tercero_cuit']
            ent_cuit = enterprises.get(ent_id, '')

            tipo_op = 'VENTA'
            emisor_cuit = ent_cuit
            receptor_cuit = tercero_cuit

            # Simple logic: if modulo is COMPRAS, it's a purchase
            # If modulo is VENTAS/VEN, it's a sale
            # If modulo is NULL or something else, we try to guess
            if modulo == 'COMPRAS':
                tipo_op = 'COMPRA'
                emisor_cuit = tercero_cuit
                receptor_cuit = ent_cuit
            elif modulo in ('VENTAS', 'VEN'):
                tipo_op = 'VENTA'
                emisor_cuit = ent_cuit
                receptor_cuit = tercero_cuit
            
            cursor.execute("""
                UPDATE erp_comprobantes 
                SET tipo_operacion = %s, emisor_cuit = %s, receptor_cuit = %s
                WHERE id = %s
            """, (tipo_op, emisor_cuit, receptor_cuit, comp_id))
        
        print(f"Normalized {len(rows)} rows.")

if __name__ == '__main__':
    normalize()
