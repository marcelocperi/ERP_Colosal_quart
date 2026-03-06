import requests
import hashlib
import os
from pathlib import Path
from dotenv import load_dotenv

# Usar ruta absoluta al .env
dotenv_path = Path(r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\.env")
load_dotenv(dotenv_path)

user = os.environ.get("FREEDNS_USER")
password = os.environ.get("FREEDNS_PASS")

sha1_hash = hashlib.sha1(f"{user}|{password}".encode()).hexdigest()
# Sin v=xml
api_url = f"https://freedns.afraid.org/api/?action=getdyndns&sha={sha1_hash}"

print(f"Probando sin v=xml: {api_url}")
r = requests.get(api_url, timeout=10)
print("Status:", r.status_code)
print("Content-Length:", len(r.text))
print("Respuesta:")
print(r.text)
