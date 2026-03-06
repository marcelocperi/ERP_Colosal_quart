
from services.georef_service import GeorefService
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("Iniciando carga de Localidades...")
    count = GeorefService.load_localidades()
    print(f"Resultado: {count} localidades cargadas.")
