
import requests
import json

def test_api():
    # Simulate a request to api_get_municipios
    # Need to know the base URL. Since I'm running on the machine, I can't easily hit the flask server if it's not running or if I don't know the port.
    # But I can test the service directly.
    import sys
    import os
    sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
    
    from services.georef_service import GeorefService
    from database import get_db_cursor
    
    # Test with a known province
    prov = "Buenos Aires"
    municipios = GeorefService.get_municipios_by_provincia_nombre(prov)
    print(f"Municipios for {prov}: {len(municipios)}")
    if municipios:
        print(f"First 5: {[m['nombre'] for m in municipios[:5]]}")
    else:
        # Check if province exists
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM sys_provincias WHERE nombre LIKE %s", (f"%{prov}%",))
            p = cursor.fetchone()
            print(f"Provincia found in DB: {p}")

if __name__ == "__main__":
    test_api()
