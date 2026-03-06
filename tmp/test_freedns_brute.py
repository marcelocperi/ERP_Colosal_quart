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

def try_auth(name, url):
    print(f"Probando {name}...")
    try:
        r = requests.get(url, timeout=10)
        print(f"Status: {r.status_code}")
        print(f"Content-Length: {len(r.text)}")
        if r.text.strip():
            print(f"Respuesta (primeros 200 chars): {r.text[:200]}")
        else:
            print("Respuesta VACIA.")
    except Exception as e:
        print(f"Error {name}: {e}")
    print("-" * 30)

combinations = [
    ("SHA1 lower(user)|pass", f"https://freedns.afraid.org/api/?action=getdyndns&v=xml&sha={hashlib.sha1(f'{user.lower()}|{password}'.encode()).hexdigest()}"),
    ("SHA1 user|pass", f"https://freedns.afraid.org/api/?action=getdyndns&v=xml&sha={hashlib.sha1(f'{user}|{password}'.encode()).hexdigest()}"),
    ("SHA1 pass", f"https://freedns.afraid.org/api/?action=getdyndns&v=xml&sha={hashlib.sha1(f'{password}'.encode()).hexdigest()}"),
    ("MD5 lower(user)|pass", f"https://freedns.afraid.org/api/?action=getdyndns&v=xml&user={hashlib.md5(f'{user.lower()}|{password}'.encode()).hexdigest()}"),
]

for name, url in combinations:
    try_auth(name, url)
