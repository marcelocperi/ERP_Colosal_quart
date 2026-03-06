import requests
import urllib3
urllib3.disable_warnings()

url = "https://fwshomo.afip.gov.ar/oconws/CONService?wsdl"
try:
    r = requests.get(url, verify=False, timeout=10)
    print(f"Status: {r.status_code}")
    print(f"Headers: {r.headers.get('Content-Type')}")
    print(f"Content (first 500 chars):\n{r.text[:500]}")
except Exception as e:
    print(f"Error: {e}")
