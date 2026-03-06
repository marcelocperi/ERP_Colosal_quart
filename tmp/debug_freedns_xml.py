import requests
import hashlib
import os
from pathlib import Path
from dotenv import load_dotenv

# Usar ruta absoluta al .env
dotenv_path = Path(r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\.env")
print(f"Buscando .env en: {dotenv_path}")
if dotenv_path.exists():
    print(".env encontrado.")
    load_dotenv(dotenv_path)
else:
    print(".env NO encontrado.")

user = os.environ.get("FREEDNS_USER")
password = os.environ.get("FREEDNS_PASS")

print(f"FREEDNS_USER: {user}")
# Ocultamos el password en el print por seguridad (solo mostramos si existe)
print(f"FREEDNS_PASS detectado: {'SI' if password else 'NO'}")

if not user or not password:
    print("Error: FREEDNS_USER o FREEDNS_PASS no están en el entorno después de cargar .env")
    exit(1)

credential_hash = hashlib.md5(f"{user}|{password}".encode()).hexdigest()
api_url = f"https://freedns.afraid.org/api/?action=getdyndns&v=xml&user={credential_hash}"

print(f"Consultando API para usuario: {user}")
try:
    r = requests.get(api_url, timeout=10)
    print("Status Code:", r.status_code)
    print("Respuesta Raw:")
    print("-" * 50)
    print(r.text)
    print("-" * 50)
except Exception as e:
    print("Error:", e)
