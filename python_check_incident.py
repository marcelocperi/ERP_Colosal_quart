import os
import sys
import json
from database import get_db_cursor
from datetime import datetime

# Forzar encoding UTF-8 para consola Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def list_incidents(limit=10):
    """Consulta los últimos incidentes registrados en sys_transaction_logs."""
    print(f" Consultando los últimos {limit} incidentes...")
    print("-" * 80)
    
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'clob_data'")
            has_clob = bool(cursor.fetchone())
            data_col = 'clob_data' if has_clob else 'error_traceback'
            
            cursor.execute(f"""
                SELECT id, module, status, incident_status, error_message, created_at
                FROM sys_transaction_logs
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            rows = cursor.fetchall()
            
            if not rows:
                print("No se encontraron incidentes.")
                return

            print(f"{'ID':<5} | {'MODULO':<15} | {'STATUS':<10} | {'FECHA':<20} | {'RESUMEN'}")
            print("-" * 80)
            for r in rows:
                created = r['created_at'].strftime('%Y-%m-%d %H:%M') if r['created_at'] else "N/A"
                msg = (r['error_message'] or "")[:40].replace('\n', ' ')
                print(f"{r['id']:<5} | {r['module']:<15} | {r['incident_status']:<10} | {created:<20} | {msg}")
    except Exception as e:
        print(f" Error al consultar incidentes: {e}")

def view_incident(incident_id):
    """Muestra el detalle completo de un incidente."""
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SHOW COLUMNS FROM sys_transaction_logs LIKE 'clob_data'")
            has_clob = bool(cursor.fetchone())
            data_col = 'clob_data' if has_clob else 'error_traceback'
            
            cursor.execute(f"""
                SELECT id, module, status, incident_status, error_message, {data_col} as detail, created_at
                FROM sys_transaction_logs
                WHERE id = %s
            """, (incident_id,))
            row = cursor.fetchone()
            
            if not row:
                print(f"No se encontró el incidente ID {incident_id}")
                return

            print(f"\nDETALLE DE INCIDENTE #{row['id']}")
            print("=" * 60)
            print(f"Módulo:      {row['module']}")
            print(f"Estado:      {row['incident_status']}")
            print(f"Fecha:       {row['created_at']}")
            print(f"Resumen:     {row['error_message']}")
            print("-" * 60)
            print("DETALLE TECNICO:")
            print(row['detail'] or "Sin detalle.")
            print("=" * 60)
    except Exception as e:
        print(f" Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1].isdigit():
            view_incident(int(sys.argv[1]))
        else:
            list_incidents()
    else:
        list_incidents()
