import sys, os, json, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_db_cursor

resolutions = {
    62: "Se corrigió el error ImportError modificando la importación 'from . import email_service' a 'from services import email_service' en core/routes.py. La acción de actualizar o tomar incidentes vuelve a estar operativa de manera global.",
    # We can also resolve the 404s conceptually if we want, but 62 is the one we actually fixed the code for.
}

with get_db_cursor(dictionary=True) as cursor:
    for err_id, resolucion_text in resolutions.items():
        cursor.execute("SELECT management_history FROM sys_transaction_logs WHERE id = %s", (err_id,))
        row = cursor.fetchone()
        if row:
            history = []
            if row['management_history']:
                try:
                    history = json.loads(row['management_history'])
                except Exception:
                    pass
            
            history.append({
                "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "user": "Antigravity (AI)",
                "note": resolucion_text,
                "status_change": "RESOLVED"
            })
            
            cursor.execute("""
                UPDATE sys_transaction_logs
                SET incident_status = 'RESOLVED',
                    management_history = %s
                WHERE id = %s
            """, (json.dumps(history), err_id))
            print(f"Incidente #{err_id} marcado como RESOLVED.")

