import os
import sys

# Add project root to sys.path
project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def close_all_incidents():
    print("Iniciando actualización de incidentes...")
    with get_db_cursor() as cursor:
        # Primero ver qué hay
        cursor.execute("SELECT COUNT(*) FROM sys_transaction_logs WHERE incident_status != 'CLOSED' OR incident_status IS NULL")
        count = cursor.fetchone()[0]
        print(f"Encontrados {count} incidentes para cerrar.")
        
        # Actualizar
        cursor.execute("UPDATE sys_transaction_logs SET incident_status = 'CLOSED' WHERE incident_status != 'CLOSED' OR incident_status IS NULL")
        print("Actualización completada.")

if __name__ == "__main__":
    close_all_incidents()
