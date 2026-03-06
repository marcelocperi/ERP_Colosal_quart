from database import get_db_cursor
import sys

with get_db_cursor(dictionary=True) as cursor:
    try:
        # Check if the column exists to prevent issues
        cursor.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'incident_status'")
        col = cursor.fetchone()
        
        if col:
            # Update all to 'CLOSED'
            cursor.execute("UPDATE sys_transaction_logs SET incident_status = 'CLOSED' WHERE incident_status != 'CLOSED' OR incident_status IS NULL")
            print(f"Éxito: Se han marcado {cursor.rowcount} incidentes como CERRADOS.")
        else:
            print("No se encontró la columna 'incident_status'. Abortando cerrado masivo.")
    except Exception as e:
        print(f"Error cerrando incidentes: {e}")
