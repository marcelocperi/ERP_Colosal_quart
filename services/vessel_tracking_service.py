import requests
import json
import logging
from datetime import datetime
from database import get_db_cursor

logger = logging.getLogger(__name__)

class VesselTrackingService:
    """
    Servicio para el seguimiento de buques utilizando la API de VesselFinder (Enfoque AIS).
    Permite rastrear barcos por MMSI o Nombre y guardar el historial en la DB.
    """
    
    API_BASE_URL = "https://api.vesselfinder.com/vessels"
    API_KEY = "PLACEHOLDER_KEY" # Se debe configurar via variable de entorno en producción

    @classmethod
    async def track_vessel_by_mmsi(cls, enterprise_id, orden_id, mmsi, user_id):
        """
        Consulta la posición y estado de un buque por su número MMSI.
        Fase 1: Conectividad y Persistencia.
        """
        if not mmsi:
            return {'success': False, 'message': 'MMSI Requerido'}

        # En modo real, haríamos la petición a VesselFinder
        # data = cls._fetch_from_api(mmsi)
        
        # Para el desarrollo inicial o si no hay API KEY, simulamos la respuesta
        # acorde a lo que devolvería una AIS API básica.
        data = cls._simulate_api_response(mmsi)
        
        if not data:
            return {'success': False, 'message': 'No se obtuvieron datos del buque'}

        # Guardar en la tabla imp_vessel_tracking
        try:
            async with get_db_cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO imp_vessel_tracking (
                        enterprise_id, orden_compra_id, vessel_mmsi, vessel_name,
                        last_lat, last_lon, eta_predicted, vessel_status,
                        last_data_received, raw_json, user_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    enterprise_id, orden_id, mmsi, data['name'],
                    data['lat'], data['lon'], data['eta'], data['status'],
                    data['tstamp'], json.dumps(data), user_id
                ))
                tracking_id = cursor.lastrowid
                
                # Sincronizar nombre del buque en la tabla de despacho
                await cursor.execute("""
                    UPDATE imp_despachos SET vessel_name = %s 
                    WHERE orden_compra_id = %s AND enterprise_id = %s
                """, (data['name'], orden_id, enterprise_id))
                
            return {
                'success': True, 
                'tracking_id': tracking_id,
                'data': data
            }
        except Exception as e:
            logger.error(f"[VesselTracking] Error guardando consulta: {e}")
            return {'success': False, 'message': str(e)}

    @classmethod
    async def get_last_tracking(cls, orden_id, enterprise_id):
        """Recupera la última posición conocida del buque para una orden."""
        async with get_db_cursor() as cursor:
            await cursor.execute("""
                SELECT * FROM imp_vessel_tracking 
                WHERE orden_compra_id = %s AND enterprise_id = %s
                ORDER BY created_at DESC LIMIT 1
            """, (orden_id, enterprise_id))
            return await cursor.fetchone()

    @classmethod
    def _simulate_api_response(cls, mmsi):
        """Simula una respuesta de la API AIS de VesselFinder."""
        # Datos ficticios basados en un buque de carga típica
        return {
            'mmsi': mmsi,
            'name': "MAERSK TACOMA",
            'lat': -34.582,
            'lon': -58.361,
            'status': "MOORED",
            'eta': "2026-03-15 14:00:00",
            'tstamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    @classmethod
    def _fetch_from_api(cls, mmsi):
        """Llamada real a la API (implementar con la Key real)."""
        params = {
            'userkey': cls.API_KEY,
            'mmsi': mmsi
        }
        try:
            resp = requests.get(cls.API_BASE_URL, params=params, timeout=10)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f"Error en llamada API: {e}")
        return None
