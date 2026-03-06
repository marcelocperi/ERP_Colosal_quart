import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor
import json

def register_georef_cron():
    with get_db_cursor() as cursor:
        cursor.execute("SELECT id FROM sys_crons WHERE nombre = 'Sincronización Completa Georef' AND enterprise_id = 0")
        if cursor.fetchone():
            print("Cron already exists.")
            return

        plan = {"days": [7], "hour": "05:00"}
        cursor.execute("""
            INSERT INTO sys_crons (nombre, descripcion, comando, frecuencia, planificacion, estado, enterprise_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            "Sincronización Completa Georef",
            "Actualiza tablas locales de Provincias, Localidades y Calles desde el API de Datos Argentina.",
            "python scripts/sync_georef.py",
            "semanal",
            json.dumps(plan),
            "activo",
            0
        ))
        print("Cron registered successfully.")

if __name__ == "__main__":
    register_georef_cron()
