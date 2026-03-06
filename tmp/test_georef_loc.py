
import requests
import json
import sys
import os

def test_localidades():
    sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
    from services.georef_service import GeorefService
    
    prov = "Buenos Aires"
    muni = "Bahía Blanca"
    localidades = GeorefService.get_localidades(prov, muni)
    print(f"Localidades for {prov}/{muni}: {len(localidades)}")
    if localidades:
        for loc in localidades[:3]:
            print(f"Loc: {loc}")
            
    # Try calling the official API directly to see if hidden fields exist
    url = f"https://apis.datos.gob.ar/georef/api/v2.0/localidades?provincia={prov}&municipio={muni}&max=5"
    resp = requests.get(url)
    data = resp.json()
    print("\nExternal API sample response:")
    print(json.dumps(data, indent=2))

if __name__ == "__main__":
    test_localidades()
