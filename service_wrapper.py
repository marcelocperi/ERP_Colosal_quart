
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import os
import subprocess
import time
import logging

# Configurar logging para el servicio
logging.basicConfig(
    filename='C:\\Users\\marce\\Documents\\GitHub\\quart\\service_status.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Colosal_ERP_Service')

class ColosalERPQuartService(win32serviceutil.ServiceFramework):
    _svc_name_ = "Colosal_ERP_Quart"
    _svc_display_name_ = "Colosal ERP (Quart Server)"
    _svc_description_ = "Provee el servidor backend asíncrono para Colosal ERP utilizando Quart/Hypercorn."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.process = None

    def SvcStop(self):
        logger.info("Recibida señal de STOP de Windows.")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        if self.process:
            logger.info(f"Terminando proceso principal (PID: {self.process.pid}).")
            # Matar el árbol de procesos para asegurar que Hypercorn muera
            subprocess.call(['taskkill', '/F', '/T', '/PID', str(self.process.pid)])
        logger.info("Servicio detenido correctamente.")

    def SvcDoRun(self):
        logger.info("Iniciando servicio Colosal ERP...")
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def main(self):
        try:
            # Rutas Base
            base_path = r'C:\Users\marce\Documents\GitHub\quart'
            venv_python = os.path.join(base_path, 'venv', 'Scripts', 'python.exe')
            app_script = os.path.join(base_path, 'app.py')

            # Variables de entorno adicionales si son necesarias
            env = os.environ.copy()
            env['PYTHONPATH'] = base_path
            env['PORT'] = '5000'
            env['FLASK_ENV'] = 'production'

            logger.info(f"Ejecutando: {venv_python} {app_script}")
            
            # Lanzamos el proceso de Quart
            self.process = subprocess.Popen(
                [venv_python, app_script],
                cwd=base_path,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )

            logger.info(f"Servidor Quart iniciado con PID {self.process.pid}")

            # Monitorear el proceso mientras dure el servicio
            while True:
                # Ver si Windows mandó el evento de STOP
                if win32event.WaitForSingleObject(self.stop_event, 1000) == win32event.WAIT_OBJECT_0:
                    break
                
                # Ver si el proceso murió solo
                if self.process.poll() is not None:
                    logger.error("!!! El servidor Quart terminó inesperadamente.")
                    break
            
        except Exception as e:
            logger.error(f"Error fatal en el servicio: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(ColosalERPQuartService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(ColosalERPQuartService)
