import requests
from database import get_db_cursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def populate_province(prov_id, prov_name):
    url = f"https://apis.datos.gob.ar/georef/api/v2.0/localidades?provincia={prov_id}&max=2000"
    try:
        logger.info(f"Downloading localities for {prov_name}...")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        localidades = data.get('localidades', [])
        logger.info(f"Found {len(localidades)} localities. Inserting into DB...")
        
        count = 0
        with get_db_cursor() as cursor:
            for l in localidades:
                muni_id = l.get('municipio', {}).get('id') if l.get('municipio') else None
                cursor.execute("""
                    INSERT INTO sys_localidades (id, nombre, provincia_id, municipio_id, centroide_lat, centroide_lon)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        nombre = VALUES(nombre),
                        provincia_id = VALUES(provincia_id),
                        municipio_id = VALUES(municipio_id),
                        centroide_lat = VALUES(centroide_lat),
                        centroide_lon = VALUES(centroide_lon)
                """, (
                    l['id'], 
                    l['nombre'], 
                    l['provincia']['id'],
                    muni_id,
                    l['centroide'].get('lat'), 
                    l['centroide'].get('lon')
                ))
                count += 1
        logger.info(f"Successfully updated {count} localities for {prov_name}.")
    except Exception as e:
        logger.error(f"Error for {prov_name}: {e}")

if __name__ == "__main__":
    populate_province('06', 'Buenos Aires')
    populate_province('02', 'CABA')
