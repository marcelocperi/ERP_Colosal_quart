@echo off
setlocal

:: Obtiene la ruta de este .bat (la carpeta multiMCP)
set "PROJECT_DIR=%~dp0"
:: El venv ahora esta dentro de la misma carpeta multiMCP
set "VENV_DIR=%PROJECT_DIR%venv"

echo ==============================================================
echo       PREPARADOR DE ENTORNO VIRTUAL - COLOSAL ERP
echo ==============================================================
echo.

:: Detectar si el python global esta disponible
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python no esta en las variables de entorno PATH de Windows.
    echo Asegurese de haber instalado Python marcando "Add python to PATH".
    pause
    exit /b 1
)

if not exist "%VENV_DIR%" (
    echo [INFO] No se detecto VENV. Creando nuevo Entorno Virtual en %VENV_DIR%...
    python -m venv "%VENV_DIR%"
) else (
    echo [INFO] Entorno Virtual detectado.
)

echo [INFO] Actualizando PIP...
"%VENV_DIR%\Scripts\python.exe" -m pip install --upgrade pip

echo [INFO] Instalando Dependencias desde requirements.txt...
"%VENV_DIR%\Scripts\python.exe" -m pip install -r "%PROJECT_DIR%requirements.txt"

echo.
echo ==============================================================
echo       ENTORNO DE PRODUCCION PREPARADO EXITOSAMENTE
echo ==============================================================
echo Si quiere ejecutar en local de prueba:
echo %VENV_DIR%\Scripts\python.exe app.py
echo.
pause
