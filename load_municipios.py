
from services.georef_service import GeorefService
import logging

logging.basicConfig(level=logging.INFO)

def main():
    print("Iniciando carga de municipios...")
    count = GeorefService.load_municipios()
    if count >= 0:
        print(f"Éxito: Se cargaron {count} municipios.")
    else:
        print("Error durante la carga de municipios.")

if __name__ == "__main__":
    main()
