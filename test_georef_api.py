import requests
try:
    url = 'https://apis.datos.gob.ar/georef/api/v2.0/localidades?provincia=06&max=20'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    for l in data.get('localidades', []):
        muni = l.get('municipio')
        muni_name = muni.get('nombre') if muni else "None"
        print(f"{l['nombre']} -> {muni_name}")
except Exception as e:
    print(f"Error: {e}")
