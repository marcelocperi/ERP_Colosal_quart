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

if not user or not password:
    print("Error: Credenciales no configuradas.")
    exit(1)

# Probar con SHA1 (más común en FreeDNS XML API)
sha1_hash = hashlib.sha1(f"{user}|{password}".encode()).hexdigest()
api_url_sha1 = f"https://freedns.afraid.org/api/?action=getdyndns&v=xml&sha={sha1_hash}"

# Probar con MD5 (por si acaso)
md5_hash = hashlib.md5(f"{user}|{password}".encode()).hexdigest()
api_url_md5 = f"https://freedns.afraid.org/api/?action=getdyndns&v=xml&user={md5_hash}"

print("Probando con SHA1...")
try:
    r = requests.get(api_url_sha1, timeout=10)
    print("SHA1 Status:", r.status_code)
    print("Respuesta SHA1:", r.text.strip())
except Exception as e:
    print("Error SHA1:", e)

print("\nProbando con MD5...")
try:
    r = requests.get(api_url_md5, timeout=10)
    print("MD5 Status:", r.status_code)
    print("Respuesta MD5:", r.text.strip())
except Exception as e:
    print("Error MD5:", e)
