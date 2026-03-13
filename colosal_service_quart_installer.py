
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import os
import subprocess
import datetime

# Configuración - Detección automática del VENV local para máxima portabilidad
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
# Ahora el venv SIEMPRE debe estar dentro de la carpeta multiMCP
VENV_DIR = os.path.join(PROJECT_DIR, "venv")

if not os.path.exists(os.path.join(VENV_DIR, "Scripts", "python.exe")):
    print(f"ERROR: No se encontró el entorno virtual en {VENV_DIR}")
    sys.exit(1)

# Determina automáticamente el ejecutable correcto del entorno virtual
PYTHON_EXE = os.path.join(VENV_DIR, "Scripts", "python.exe")
SCRIPT_PATH = os.path.join(PROJECT_DIR, "run_hypercorn.py")

def ensure_dlls():
    """Copia las DLLs necesarias al venv para que pythonservice.exe funcione (Evita Error 1053)"""
    try:
        import shutil
        cfg_path = os.path.join(VENV_DIR, "pyvenv.cfg")
        home_path = ""
        if os.path.exists(cfg_path):
            with open(cfg_path, 'r') as f:
                for line in f:
                    if line.startswith('home ='):
                        home_path = line.split('=')[1].strip()
                        break
        
        if home_path:
            # Lista de DLLs críticas para Python 3.14
            for dll in ["python3.dll", "python314.dll"]:
                src = os.path.join(home_path, dll)
                dst = os.path.join(VENV_DIR, dll)
                if os.path.exists(src) and not os.path.exists(dst):
                    print(f"[INFO] Copiando {dll} al VENV para estabilidad del servicio...")
                    shutil.copy2(src, dst)
    except Exception as e:
        print(f"[WARN] No se pudieron copiar las DLLs: {e}")

# Asegurar entorno antes de continuar
if os.path.exists(VENV_DIR):
    ensure_dlls()


class ColosalQuartService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ERP_Colosal_Quart"
    _svc_display_name_ = "ERP Colosal Quart (Servicio)"
    _svc_description_ = "Motor de producción para Colosal ERP (Hypercorn/Quart ASGI)"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.process = None

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        if self.process:
            try:
                self.process.terminate()
            except:
                pass
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        # Reportar que el servicio está iniciando
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        
        # Reportar que ya está ejecutándose ANTES de entrar al bucle principal
        # Esto previene el error 1053 de Windows
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        self.main()

    def main(self):
        try:
            log_dir = os.path.join(PROJECT_DIR, "logs")
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            stdout_path = os.path.join(log_dir, "service_quart_stdout.log")
            stderr_path = os.path.join(log_dir, "service_quart_stderr.log")

            with open(stdout_path, "a", encoding='utf-8') as out, \
                 open(stderr_path, "a", encoding='utf-8') as err:
                
                now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                out.write(f"\n--- Iniciando Proceso Colosal Quart ERP a las {now_str} ---\n")
                out.flush()
                
                # Ejecutar hypercorn
                # Usamos un conjunto de banderas para asegurar que el proceso sea hijo del servicio
                self.process = subprocess.Popen([PYTHON_EXE, SCRIPT_PATH], 
                                             cwd=PROJECT_DIR,
                                             stdout=out,
                                             stderr=err,
                                             creationflags=subprocess.CREATE_NO_WINDOW)
                
                out.write(f"Proceso iniciado con PID: {self.process.pid}\n")
                out.flush()

                # Mientras el proceso esté vivo y no pidan parar...
                while self.process.poll() is None:
                    # Esperar la señal de stop pero con un pequeño timeout para chequear el proceso
                    rc = win32event.WaitForSingleObject(self.hWaitStop, 5000)
                    if rc == win32event.WAIT_OBJECT_0:
                        break
                
                if self.process.poll() is not None:
                    out.write(f"[{datetime.datetime.now()}] El subproceso terminó inesperadamente con código {self.process.returncode}\n")
                    out.flush()
                    
        except Exception as e:
            servicemanager.LogErrorMsg(f"Error en el servicio: {str(e)}")
        
        # Al salir, asegurar que el proceso se detenga
        if self.process and self.process.poll() is None:
            self.process.terminate()

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1].lower() == 'install':
        # --- Lógica Prolija para Instalación en VENV ---
        # 1. Encontrar el Host real (el que está junto a python314.dll)
        # Normalmente está en el 'home' definido en pyvenv.cfg
        import winreg
        
        # Intentar obtener el home del venv
        home_path = ""
        cfg_path = os.path.join(VENV_DIR, "pyvenv.cfg")
        if os.path.exists(cfg_path):
            with open(cfg_path, 'r') as f:
                for line in f:
                    if line.startswith('home ='):
                        home_path = line.split('=')[1].strip()
                        break
        
        if not home_path:
            # Fallback al ejecutable actual si no hay cfg
            home_path = os.path.dirname(sys.executable)

        # El host que buscamos
        global_host = os.path.join(home_path, "pythonservice.exe")
        
        if os.path.exists(global_host):
            print(f"[PRO] Usando Host Global para evitar error 1053: {global_host}")
            # Registramos el servicio
            win32serviceutil.HandleCommandLine(ColosalQuartService)
            
            # Ajustamos el ImagePath en el registro para que apunte al host que SI tiene las DLLs
            key_path = f"SYSTEM\\CurrentControlSet\\Services\\{ColosalQuartService._svc_name_}"
            try:
                key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path, 0, winreg.KEY_SET_VALUE)
                # El comando original tiene argumentos, los preservamos
                img_path = f'"{global_host}"'
                winreg.SetValueEx(key, "ImagePath", 0, winreg.REG_EXPAND_SZ, img_path)
                winreg.CloseKey(key)
                
                # Seteamos el PYTHONPATH para que el servicio vea las librerías del VENV
                param_key_path = f"{key_path}\\Parameters"
                # Crear la clave si no existe
                try:
                    param_key = winreg.CreateKey(winreg.HKEY_LOCAL_MACHINE, param_key_path)
                except:
                    param_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, param_key_path, 0, winreg.KEY_SET_VALUE)
                
                # El path incluye la carpeta del proyecto y el site-packages del venv
                extra_paths = [
                    PROJECT_DIR,
                    os.path.join(VENV_DIR, "Lib", "site-packages")
                ]
                winreg.SetValueEx(param_key, "PythonPath", 0, winreg.REG_SZ, ";".join(extra_paths))
                winreg.CloseKey(param_key)
                print("[PRO] Registro optimizado con PYTHONPATH del VENV.")
            except Exception as e:
                print(f"[ERROR] No se pudo optimizar el registro: {e}")
        else:
            print("[WARN] No se encontró pythonservice.exe global. Procediendo con instalación estándar...")
            win32serviceutil.HandleCommandLine(ColosalQuartService)
    else:
        if len(sys.argv) > 1:
            win32serviceutil.HandleCommandLine(ColosalQuartService)
        else:
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(ColosalQuartService)
            servicemanager.StartServiceCtrlDispatcher()

