"""
FreeDNS Dynamic DNS Updater — Colosal ERP
==========================================
Soporta dos modos:
  1. TOKEN directo  → FREEDNS_TOKEN en .env
  2. API XML        → FREEDNS_USER + FREEDNS_PASS en .env
                      Obtiene TODOS los subdominios automáticamente.

Ejecutar: python freedns_updater.py
Log:      logs/freedns.log
"""

import requests
import hashlib
import time
import logging
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from dotenv import load_dotenv

# ── Cargar .env ────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent / ".env")

FREEDNS_TOKEN   = os.environ.get("FREEDNS_TOKEN", "")    # Modo 1: token directo
FREEDNS_USER    = os.environ.get("FREEDNS_USER", "")     # Modo 2: API XML
FREEDNS_PASS    = os.environ.get("FREEDNS_PASS", "")     # Modo 2: API XML

UPDATE_INTERVAL = int(os.environ.get("FREEDNS_INTERVAL", 300))  # segundos (default 5 min)

# ── Logging ────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FreeDNS] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "freedns.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_api_domains(user: str, password: str) -> list[dict]:
    """
    Consulta la API de FreeDNS y retorna la lista de subdominios.
    Cada elemento: { 'host': 'sub.dominio.com', 'url': 'https://sync.afraid.org/u/TOKEN/' }
    """
    credential_hash = hashlib.sha1(f"{user}|{password}".encode()).hexdigest()
    api_url = f"https://freedns.afraid.org/api/?action=getdyndns&sha={credential_hash}"

    try:
        r = requests.get(api_url, timeout=10)
        r.raise_for_status()

        domains = []
        # FreeDNS responde: host|ip|url (una por linea)
        for line in r.text.strip().splitlines():
            parts = line.split("|")
            if len(parts) >= 3:
                domains.append({
                    "host": parts[0].strip(),
                    "current_ip": parts[1].strip(),
                    "url": parts[2].strip()
                })

        logger.info(f"API: {len(domains)} subdominio(s) encontrado(s)")
        for d in domains:
            logger.info(f"  \u2192 {d['host']} (IP en DNS: {d['current_ip']})")
        return domains

    except Exception as e:
        logger.error(f"Error consultando API: {e}")
        return []


def update_via_api(user: str, password: str) -> bool:
    """Actualiza TODOS los subdominios usando la API."""
    domains = get_api_domains(user, password)
    if not domains:
        return False

    success = True
    for domain in domains:
        try:
            r = requests.get(domain["url"], timeout=10)
            resp = r.text.strip()
            logger.info(f"{domain['host']}: {resp}")
            if "Updated" in resp or "No IP change" in resp or "has not changed" in resp:
                pass
            else:
                success = False
        except Exception as e:
            logger.error(f"Error actualizando {domain['host']}: {e}")
            success = False

    return success


# ══════════════════════════════════════════════════════════════════════════════
#  MODO 1 — TOKEN DIRECTO
# ══════════════════════════════════════════════════════════════════════════════

def update_via_token(token: str) -> bool:
    """Actualiza usando el token directo del Direct URL."""
    url = f"https://sync.afraid.org/u/{token}/"
    try:
        r = requests.get(url, timeout=10)
        resp = r.text.strip()
        logger.info(f"FreeDNS token response: {resp}")
        return "Updated" in resp or "No IP change" in resp or "has not changed" in resp
    except Exception as e:
        logger.error(f"Error actualizando via token: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  IP PÚBLICA
# ══════════════════════════════════════════════════════════════════════════════

def get_public_ip() -> str:
    services = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
    ]
    for url in services:
        try:
            r = requests.get(url, timeout=5)
            ip = r.text.strip()
            if ip:
                return ip
        except Exception:
            continue
    raise RuntimeError("No se pudo obtener la IP pública")


# ══════════════════════════════════════════════════════════════════════════════
#  LOOP PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run_updater():
    # Determinar modo
    use_xml_api = bool(FREEDNS_USER and FREEDNS_PASS)
    use_token   = bool(FREEDNS_TOKEN and FREEDNS_TOKEN != "PEGAR_AQUI_TU_TOKEN")

    if not use_xml_api and not use_token:
        logger.error(
            "No hay credenciales configuradas. Editá el .env:\n"
            "  Modo XML:   FREEDNS_USER + FREEDNS_PASS\n"
            "  Modo Token: FREEDNS_TOKEN"
        )
        return

    modo = "API XML (todos los subdominios)" if use_xml_api else "Token directo"
    logger.info("=" * 55)
    logger.info("  FreeDNS Updater — Colosal ERP")
    logger.info(f"  Modo: {modo}")
    logger.info(f"  Intervalo: {UPDATE_INTERVAL}s")
    logger.info("=" * 55)

    if use_xml_api:
        logger.info("Consultando subdominios disponibles...")
        get_api_domains(FREEDNS_USER, FREEDNS_PASS)

    last_ip = None

    # Bucle desactivado por seguridad de red (solicitud del usuario)
    logger.warning("FreeDNS Updater DESACTIVADO por configuración del usuario (riesgo de red).")
    return

    while True:
        try:
            current_ip = get_public_ip()

            if current_ip != last_ip:
                logger.info(f"IP cambió: {last_ip or 'primera vez'} → {current_ip}")

                if use_xml_api:
                    ok = update_via_api(FREEDNS_USER, FREEDNS_PASS)
                else:
                    ok = update_via_token(FREEDNS_TOKEN)

                if ok:
                    last_ip = current_ip
                    logger.info(f"✅ DNS actualizado → {current_ip}")
                else:
                    logger.error("❌ Falló la actualización")
            else:
                logger.debug(f"IP sin cambios: {current_ip}")

        except Exception as e:
            logger.error(f"Error en ciclo: {e}")

        time.sleep(UPDATE_INTERVAL)


if __name__ == "__main__":
    run_updater()
