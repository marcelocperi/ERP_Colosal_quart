import sys
import os
import logging
from datetime import datetime

# Añadir el path raíz para importaciones de módulos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_db_cursor
from services import finance_service

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_dolar():
    """
    Tarea de fondo (Cron) para actualizar el valor del Dólar Oficial BNA/BCRA.
    Se recomienda ejecutar cada 15 o 30 minutos desde el Gestor de Crons.
    """
    from datetime import datetime
    from database import get_db_cursor, calcular_proxima_ejecucion
    cron_name = "Servicio Cotización Dólar"
    logger.info("Iniciando sincronización automática del Dólar BCRA...")
    try:
        import requests
        url = "https://dolarapi.com/v1/dolares/oficial"
        response = requests.get(url, timeout=5)
        data = response.json()
        
        # Obtener todas las empresas activas y actualizar su cotización
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT id FROM sys_enterprises")
            enterprises = cursor.fetchall()
            
        with get_db_cursor() as cursor:
            for emp in enterprises:
                cursor.execute("""
                    INSERT INTO cotizacion_dolar (enterprise_id, compra, venta, casa, nombre, moneda, fechaActualizacion, origen)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (emp['id'], data['compra'], data['venta'], data['casa'], data['nombre'], data['moneda'], data['fechaActualizacion'], 'cron'))
            cursor.connection.commit()
            
        logger.info(f"[OK] Dólar oficial sincronizado en bloque para {len(enterprises)} empresas. compra={data.get('compra')} venta={data.get('venta')}")

    except Exception as e:
        logger.error(f"[ERR] Excepción no controlada al actualizar dólar: {e}")
    finally:
        try:
            now = datetime.now()
            with get_db_cursor(dictionary=True) as cursor:
                cursor.execute("SELECT id, frecuencia, planificacion FROM sys_crons WHERE nombre = %s", (cron_name,))
                cron_row = cursor.fetchone()
            if cron_row:
                proxima = calcular_proxima_ejecucion(cron_row['frecuencia'], cron_row['planificacion'])
                with get_db_cursor() as cursor:
                    cursor.execute(
                        "UPDATE sys_crons SET ultima_ejecucion = %s, proxima_ejecucion = %s WHERE id = %s",
                        (now, proxima, cron_row['id'])
                    )
                    cursor.connection.commit()
        except Exception as ex:
            logger.error(f"[ERR] No se pudo actualizar sys_crons: {ex}")

if __name__ == "__main__":
    update_dolar()
