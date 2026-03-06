
# INSTRUCCIONES PARA INSTALAR COMO SERVICIO DE WINDOWS
# Proyecto: Colosal ERP

He preparado el entorno para que la aplicación sea estable y profesional tanto en Windows como en Linux. Finalmente, hemos optado por un instalador nativo de Python para mayor compatibilidad.

### 1. Cambios realizados para Estabilidad:
-   **Servidor de Producción (Waitress)**: Se usa `waitress` para garantizar estabilidad 24/7 en el puerto **5000**.
-   **Script de Lanzamiento**: El punto de entrada oficial es `run_production.py`.
-   **Wrapper de Servicio**: Se utiliza `colosal_service_installer.py` (basado en `pywin32`) para integrarse con el Service Control Manager de Windows.

### 2. Gestión del Servicio en Windows:

Para gestionar el servicio, debes abrir **PowerShell como Administrador** y usar los siguientes comandos:

**Para Instalar/Actualizar:**
```powershell
cd "C:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
python colosal_service_installer.py install
python colosal_service_installer.py --startup auto start
```

**Para Detener / Iniciar / Eliminar:**
- **Detener**: `python colosal_service_installer.py stop`
- **Iniciar**: `python colosal_service_installer.py start`
- **Eliminar**: `python colosal_service_installer.py remove`

**Gestión Visual:**
Puedes abrir la herramienta **Servicios** (`services.msc`) de Windows y buscar **Colosal ERP (Servicio)** para monitorizar su estado o reiniciarlo gráficamente.

### 3. Logs y Diagnóstico:
Si el servicio no responde, revisa la carpeta `logs/`:
- `service_stdout.log`: Registro de inicios y actividad del servidor.
- `service_stderr.log`: Errores críticos de Windows o de ejecución de Python.
- `server_production.log`: Logs específicos de la aplicación Flask.

### 4. Notas para Linux:
Si decides migrar a Linux, el archivo `requirements.txt` instalará automáticamente `gunicorn`. Podrás usar systemd con el siguiente comando:
`gunicorn -w 4 -b 0.0.0.0:5000 app:app`
