@echo off
setlocal

:: Obtiene la ruta de este .bat (la carpeta multiMCP)
set "PROJECT_DIR=%~dp0"
:: El venv ahora esta dentro de la misma carpeta multiMCP
set "VENV_DIR=%PROJECT_DIR%venv"

echo ==============================================================
echo       INSTALADOR SERVICIO WINDOWS - COLOSAL ERP
echo ==============================================================
echo [INFO] Carpeta Proyecto: %PROJECT_DIR%
echo [INFO] Carpeta VENV: %VENV_DIR%
echo.

if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo [ERROR] No se encontro "%VENV_DIR%\Scripts\python.exe"
    echo Por favor ejecute "1_setup_env_windows.bat" primero.
    pause
    exit /b 1
)

:: IMPORTANTE: Es necesario correr pywin32_postinstall
echo [INFO] Inicializando librerias de integracion de Windows (pywin32)...
"%VENV_DIR%\Scripts\python.exe" "%VENV_DIR%\Scripts\pywin32_postinstall.py" -install -quiet >nul 2>&1

set "VIRTUAL_ENV=%VENV_DIR%"

echo [INFO] Removiendo servicio previo (si existe)...
"%VENV_DIR%\Scripts\python.exe" "%PROJECT_DIR%colosal_service_installer.py" remove >nul 2>&1

echo [INFO] Registrando Windows Service...
"%VENV_DIR%\Scripts\python.exe" "%PROJECT_DIR%colosal_service_installer.py" --startup auto install

echo [INFO] Iniciando motor en segundo plano...
"%VENV_DIR%\Scripts\python.exe" "%PROJECT_DIR%colosal_service_installer.py" start

echo.
echo ==============================================================
echo             SERVICIO INICIADO Y FUNCIONANDO!               
echo ==============================================================
pause
