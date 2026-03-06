import sys
import os
import logging
from datetime import datetime

# Añadir el path raíz para importaciones de módulos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_cursor
from services.vessel_tracking_service import VesselTrackingService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_vessel_update():
    """
    Tarea de fondo (Cron) para actualizar el estado de todos los buques activos.
    Se recomienda ejecutar 1 vez al día (o cada 12hs).
    """
    logger.info("Iniciando actualización automática de buques AIS...")
    
    with get_db_cursor() as cursor:
        # 1. Definición de "Flujo Abierto" para rastreo satelital:
        # - El barco tiene un MMSI asignado.
        # - NO se ha registrado aún la Fecha de Arribo Real (ATA).
        # - La orden no ha sido finalizada completamente (INGRESADO).
        # Esto permite seguir el buque aunque ya esté en puerto o esperando canal, 
        # hasta que el usuario confirme el arribo real.
        cursor.execute("""
            SELECT id, enterprise_id, orden_compra_id, vessel_mmsi, vessel_name 
            FROM imp_despachos 
            WHERE (fecha_arribo_real IS NULL OR fecha_arribo_real = '0000-00-00')
              AND vessel_mmsi IS NOT NULL 
              AND vessel_mmsi != ''
              AND estado != 'INGRESADO'
        """)
        active_trackings = cursor.fetchall()
        
    if not active_trackings:
        logger.info("No hay buques pendientes de arribo con MMSI configurado. Finalizando tarea.")
        return

    logger.info(f"Se encontraron {len(active_trackings)} despachos para actualizar.")
    
    updated_count = 0
    error_count = 0
    
    for row in active_trackings:
        try:
            # Usamos un user_id ficticio (0 o un ID de sistema) para el log de 'creado por'
            res = VesselTrackingService.track_vessel_by_mmsi(
                enterprise_id=row['enterprise_id'],
                orden_id=row['orden_compra_id'],
                mmsi=row['vessel_mmsi'],
                user_id=1 # Admin/System
            )
            
            if res['success']:
                logger.info(f"[OK] Sincronizado: {row['vessel_name'] or row['vessel_mmsi']} (Orden #{row['orden_compra_id']})")
                updated_count += 1
            else:
                logger.warning(f"[SKIP] {row['vessel_mmsi']}: {res['message']}")
                error_count += 1
                
        except Exception as e:
            logger.error(f"[ERR] Error actualizando Buque {row['vessel_mmsi']}: {e}")
            error_count += 1

    logger.info(f"Proceso AIS finalizado. Exitos: {updated_count}, Errores/Omitidos: {error_count}")

    # 2. Procesar Alertas de Demora (Vencimiento de días libres de puerto)
    try:
        from services.importacion_service import ImportacionService
        ImportacionService.procesar_alertas_demora()
    except Exception as e:
        logger.error(f"[ERR] Error procesando alertas de demora: {e}")

if __name__ == "__main__":
    run_vessel_update()
