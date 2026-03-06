import sys
import os
project_root = r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP'
sys.path.append(project_root)

from database import get_db_cursor

def explore_sales_tables():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SHOW TABLES LIKE 'vta_%'")
        vta_tables = cursor.fetchall()
        cursor.execute("SHOW TABLES LIKE 'erp_comprobantes%'")
        erp_tables = cursor.fetchall()
        cursor.execute("SHOW TABLES LIKE 'stk_%'")
        stk_tables = cursor.fetchall()
        
        print("VTA TABLES:")
        for t in vta_tables: print(f"- {list(t.values())[0]}")
        
        print("\nERP TABLES (Comprobantes):")
        for t in erp_tables: print(f"- {list(t.values())[0]}")
        
        # Checking for client table
        cursor.execute("SHOW TABLES LIKE '%cliente%'")
        client_tables = cursor.fetchall()
        print("\nCLIENT TABLES:")
        for t in client_tables: print(f"- {list(t.values())[0]}")

        # Checking schemas for critical sales tables
        critical_tables = ['vta_pedidos', 'vta_detalles_pedido', 'erp_comprobantes', 'erp_comprobantes_detalle', 'vta_clientes']
        for t in critical_tables:
            try:
                cursor.execute(f"DESCRIBE {t}")
                print(f"\nSCHEMA {t}:")
                for row in cursor.fetchall():
                    print(f"  {row['Field']:<20} | {row['Type']:<20}")
            except:
                print(f"\nTable {t} not found or error describing.")

if __name__ == "__main__":
    explore_sales_tables()
