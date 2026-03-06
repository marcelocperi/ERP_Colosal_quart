"""
setup_ollama_service.py
=======================
Instala Ollama como servicio de Windows usando SimpleServiceManager (SSM).
  → https://github.com/koleys/SimpleServiceManager

  SSM es la alternativa moderna a NSSM (.NET 8).
  Actúa como wrapper Win32 para cualquier .exe, resolviendo el ERROR 1053
  que ocurre al intentar registrar ollama.exe directamente con 'sc create'.

Requiere: ejecutar como ADMINISTRADOR
"""

import os
import sys
import json
import time
import shutil
import ctypes
import zipfile
import tempfile
import subprocess
import urllib.request

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
SERVICE_NAME   = "OllamaLocalIA"
DISPLAY_NAME   = "Ollama Local AI Engine (Colosal)"
SERVICE_DESC   = "Motor de IA local Ollama para el sistema Colosal ERP"

SSM_DOWNLOAD   = "https://github.com/koleys/SimpleServiceManager/releases/download/v1.1.0/SimpleServiceManager1.1.0.zip"
SSM_FOLDER     = os.path.join(os.environ.get("PROGRAMDATA", "C:\\ProgramData"), "SimpleServiceManager", "Ollama")
SSM_EXE        = os.path.join(SSM_FOLDER, "SimpleServiceManager.exe")
SSM_SETTINGS   = os.path.join(SSM_FOLDER, "appsettings.json")

LOG_FOLDER     = os.path.join(os.environ.get("PROGRAMDATA", "C:\\ProgramData"), "OllamaService")


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except Exception:
        return False


def find_ollama():
    """Busca ollama.exe en rutas estándar de Windows."""
    local_app_data  = os.environ.get("LOCALAPPDATA", "")
    program_files   = os.environ.get("ProgramFiles",    "C:\\Program Files")
    program_files86 = os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")

    candidates = [
        shutil.which("ollama.exe"),
        os.path.join(local_app_data,  "Programs", "Ollama", "ollama.exe"),
        os.path.join(local_app_data,  "Ollama", "ollama.exe"),
        os.path.join(program_files,   "Ollama", "ollama.exe"),
        os.path.join(program_files86, "Ollama", "ollama.exe"),
        "C:\\ollama\\ollama.exe",
    ]
    for p in candidates:
        if p and os.path.isfile(p):
            return os.path.abspath(p)
    return None


def download_ssm():
    """Descarga y extrae SimpleServiceManager en SSM_FOLDER."""
    print(f"  Descargando SimpleServiceManager desde GitHub...")
    zip_path = os.path.join(tempfile.gettempdir(), "SimpleServiceManager.zip")

    try:
        urllib.request.urlretrieve(SSM_DOWNLOAD, zip_path)
        print(f"  Descargado en: {zip_path}")
    except Exception as e:
        print(f"  ✗ Error al descargar: {e}")
        return False

    os.makedirs(SSM_FOLDER, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(SSM_FOLDER)
        print(f"  Extraído en: {SSM_FOLDER}")
    except Exception as e:
        print(f"  ✗ Error al extraer ZIP: {e}")
        return False

    # El ZIP puede extraer en subcarpeta — buscar el exe
    if not os.path.isfile(SSM_EXE):
        for root, dirs, files in os.walk(SSM_FOLDER):
            for f in files:
                if f.lower() == "simpleservicemanager.exe":
                    src = os.path.join(root, f)
                    shutil.copy2(src, SSM_EXE)
                    # Copiar también appsettings.json junto al exe
                    settings_src = os.path.join(root, "appsettings.json")
                    if os.path.isfile(settings_src):
                        shutil.copy2(settings_src, SSM_SETTINGS)
                    print(f"  SimpleServiceManager.exe copiado a: {SSM_FOLDER}")
                    return True
        print("  ✗ No se encontró SimpleServiceManager.exe dentro del ZIP.")
        return False

    return True


def find_or_get_ssm():
    """Devuelve la ruta al SSM exe, descargándolo si no existe."""
    if os.path.isfile(SSM_EXE):
        print(f"  SSM ya disponible en: {SSM_EXE}")
        return SSM_EXE

    if download_ssm():
        if os.path.isfile(SSM_EXE):
            return SSM_EXE

    return None


def configure_ssm(ollama_path):
    """
    Escribe appsettings.json junto a SimpleServiceManager.exe
    con la configuración de Ollama.
    """
    config = {
        "Configs": {
            "AppPath": ollama_path,          # Ruta al ejecutable
            "AppParams": "serve",            # Parámetro: 'ollama serve'
            "RestartAppAutomatically": True, # Reiniciar automáticamente si falla
            "RestartDelay": 5000             # 5 segundos antes de reiniciar
        }
    }

    os.makedirs(SSM_FOLDER, exist_ok=True)
    with open(SSM_SETTINGS, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)

    print(f"  appsettings.json configurado:")
    print(f"    AppPath   : {ollama_path}")
    print(f"    AppParams : serve")
    print(f"    AutoRestart: True (delay 5s)")


def kill_existing_ollama():
    """Mata cualquier proceso ollama.exe activo."""
    r = subprocess.run(
        "taskkill /IM ollama.exe /F",
        capture_output=True, text=True, shell=True
    )
    if "SUCCESS" in r.stdout or "ÉXITO" in r.stdout or "correcto" in r.stdout.lower():
        print("  Proceso ollama.exe previo terminado.")
    else:
        print("  No había proceso ollama.exe activo.")


def remove_existing_service():
    """Detiene y elimina el servicio si existe."""
    subprocess.run(f"sc stop {SERVICE_NAME}",   capture_output=True, shell=True)
    time.sleep(2)
    subprocess.run(f"sc delete {SERVICE_NAME}", capture_output=True, shell=True)
    time.sleep(1)
    print(f"  Servicio '{SERVICE_NAME}' previo eliminado (si existía).")


def install_service(ssm_exe):
    """
    Registra SimpleServiceManager.exe como servicio de Windows.
    SSM lee appsettings.json desde su misma carpeta y lanza ollama serve.
    """
    # Crear servicio
    create_cmd = (
        f'sc create "{SERVICE_NAME}" '
        f'start= auto '
        f'binPath= "{ssm_exe}"'
    )
    r = subprocess.run(create_cmd, capture_output=True, text=True, shell=True)
    if r.returncode != 0:
        print(f"  ✗ Error al crear el servicio:\n    {r.stdout.strip()} {r.stderr.strip()}")
        return False

    # Descripción
    subprocess.run(
        f'sc description "{SERVICE_NAME}" "{SERVICE_DESC}"',
        capture_output=True, shell=True
    )

    # Recuperación automática
    subprocess.run(
        f'sc failure "{SERVICE_NAME}" reset= 0 actions= restart/10000/restart/30000/restart/60000',
        capture_output=True, shell=True
    )

    print(f"  ✓ Servicio '{SERVICE_NAME}' registrado con SimpleServiceManager.")
    return True


def start_and_verify():
    """Inicia el servicio y verifica que quede RUNNING."""
    r = subprocess.run(
        f"sc start {SERVICE_NAME}",
        capture_output=True, text=True, shell=True
    )
    time.sleep(4)

    status = subprocess.run(
        f"sc query {SERVICE_NAME}",
        capture_output=True, text=True, shell=True
    )

    if "RUNNING" in status.stdout:
        return True

    print(f"\n  Estado actual del servicio:\n{status.stdout.strip()}")

    # Buscar logs de SSM (SSM los crea junto al exe)
    for log_name in ["log.txt", "error.log", "logs\\log.txt"]:
        log_path = os.path.join(SSM_FOLDER, log_name)
        if os.path.isfile(log_path):
            try:
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()[-20:]
                print(f"\n  Log SSM ({log_path}):")
                print("  " + "  ".join(lines))
            except Exception:
                pass

    return False


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 62)
    print("  Instalador de Ollama como Servicio de Windows")
    print("  Wrapper: SimpleServiceManager (koleys/SimpleServiceManager)")
    print("=" * 62)

    # Verificar privilegios
    if not is_admin():
        print("\n⚠  ERROR: Debes ejecutar este script como ADMINISTRADOR.")
        print("   → Click derecho sobre PowerShell/CMD → 'Ejecutar como administrador'")
        print(f"   → cd {os.path.dirname(os.path.abspath(__file__))}")
        print("   → python setup_ollama_service.py")
        sys.exit(1)

    # [1] Localizar Ollama
    print("\n[1/6] Buscando ollama.exe ...")
    ollama_path = find_ollama()
    if not ollama_path:
        print("  ✗ No se encontró ollama.exe.")
        print("  → Descarga Ollama desde https://ollama.com/download/OllamaSetup.exe")
        sys.exit(1)
    print(f"  ✓ Ollama: {ollama_path}")

    # [2] Obtener SimpleServiceManager
    print("\n[2/6] Obteniendo SimpleServiceManager (wrapper SSM) ...")
    ssm_exe = find_or_get_ssm()
    if not ssm_exe:
        print("  ✗ No se pudo obtener SimpleServiceManager.")
        print("  → Descarga manualmente desde:")
        print("    https://github.com/koleys/SimpleServiceManager/releases")
        print(f"   y extrae en: {SSM_FOLDER}")
        sys.exit(1)
    print(f"  ✓ SSM: {ssm_exe}")

    # [3] Configurar appsettings.json
    print("\n[3/6] Configurando appsettings.json ...")
    configure_ssm(ollama_path)

    # [4] Matar instancias previas de Ollama
    print("\n[4/6] Cerrando instancias previas de Ollama ...")
    kill_existing_ollama()

    # [5] Limpiar e instalar servicio
    print("\n[5/6] Instalando el servicio ...")
    remove_existing_service()
    if not install_service(ssm_exe):
        sys.exit(1)

    # [6] Iniciar y verificar
    print("\n[6/6] Iniciando el servicio ...")
    ok = start_and_verify()

    # Resultado final
    print("\n" + "=" * 62)
    if ok:
        print("  ✅ ¡LISTO! Ollama corre como servicio de Windows.")
        print(f"     Servicio  : {SERVICE_NAME}")
        print(f"     Wrapper   : SimpleServiceManager (.NET 8)")
        print(f"     Endpoint  : http://localhost:11434")
        print(f"     Inicio    : Automático (al arrancar Windows)")
        print()
        print("  Comandos útiles:")
        print(f"    sc query {SERVICE_NAME}   → estado")
        print(f"    sc stop  {SERVICE_NAME}   → detener")
        print(f"    sc start {SERVICE_NAME}   → iniciar")
        print(f"    services.msc               → panel gráfico")
    else:
        print("  ⚠  El servicio se instaló pero no arrancó inmediatamente.")
        print("     Podés iniciarlo manualmente:")
        print(f"       sc start {SERVICE_NAME}")
        print("     O abriendo: services.msc → OllamaLocalIA → Iniciar")
        print()
        print(f"  Configuración SSM : {SSM_SETTINGS}")
        print(f"  Exe SSM           : {ssm_exe}")
    print("=" * 62)


if __name__ == "__main__":
    main()
