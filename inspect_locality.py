import requests
try:
    # Use a province that responds quickly
    url = 'https://apis.datos.gob.ar/georef/api/v2.0/localidades?max=1'
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    print(response.json()['localidades'][0])
except Exception as e:
    print(f"Error: {e}")
