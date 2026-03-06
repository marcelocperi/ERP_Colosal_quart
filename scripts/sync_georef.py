
import os
import sys
import datetime
import json
import logging

# Asegurar que el root del proyecto esté en el path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from services.georef_service import GeorefService
from database import get_db_cursor

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def main():
    cron_name = "Sincronización Completa Georef"
    start_time = datetime.datetime.now()
    log_id = None
    cron_id = None

    try:
        # 1. Identificar el CRON y crear log
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT id FROM sys_crons WHERE nombre = %s", (cron_name,))
            cron = cursor.fetchone()
            if cron:
                cron_id = cron['id']
                cursor.execute("""
                    INSERT INTO sys_crons_logs (cron_id, fecha_inicio, status, resultado)
                    VALUES (%s, %s, %s, %s)
                """, (cron_id, start_time, 'exito', 'Sincronización en curso...'))
                log_id = cursor.lastrowid
                cursor.connection.commit()

        logger.info(f"Iniciando tarea programada: {cron_name}...")
        
        # Ejecución real
        GeorefService.sync_full()
        
        # 2. Finalizar con éxito
        if log_id:
            end_time = datetime.datetime.now()
            with get_db_cursor(dictionary=True) as cursor:
                cron_row = cursor.fetchone() if False else None
                cursor.execute("SELECT frecuencia, planificacion FROM sys_crons WHERE id = %s", (cron_id,))
                cron_row = cursor.fetchone()
            from database import calcular_proxima_ejecucion
            proxima = calcular_proxima_ejecucion(cron_row['frecuencia'], cron_row['planificacion']) if cron_row else None
            with get_db_cursor() as cursor:
                cursor.execute("""
                    UPDATE sys_crons_logs 
                    SET fecha_fin = %s, status = %s, resultado = %s
                    WHERE id = %s
                """, (end_time, 'exito', 'Sincronización completada exitosamente.', log_id))
                cursor.execute(
                    "UPDATE sys_crons SET ultima_ejecucion = %s, proxima_ejecucion = %s WHERE id = %s",
                    (end_time, proxima, cron_id)
                )
                cursor.connection.commit()
        
        logger.info("Sincronización Georef completada exitosamente.")

    except Exception as e:
        error_msg = f"Error fatal en la sincronización Georef: {str(e)}"
        logger.error(error_msg)
        
        if log_id:
            try:
                with get_db_cursor() as cursor:
                    cursor.execute("""
                        UPDATE sys_crons_logs 
                        SET fecha_fin = %s, status = %s, resultado = %s
                        WHERE id = %s
                    """, (datetime.datetime.now(), 'error', error_msg, log_id))
                    cursor.connection.commit()
            except: pass
        sys.exit(1)

if __name__ == "__main__":
    main()
