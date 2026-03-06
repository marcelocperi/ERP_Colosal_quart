
from services.georef_service import GeorefService
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("Iniciando carga de provincias desde API Georef...")
    count = GeorefService.load_provincias()
    if count >= 0:
        print(f"Éxito: Se procesaron {count} provincias.")
    else:
        print("Error al procesar la carga de provincias.")
