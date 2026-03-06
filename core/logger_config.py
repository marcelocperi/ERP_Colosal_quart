
import logging
import logging.handlers
import os
import sys

# Directorio de logs
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger(name, log_file=None, level=logging.INFO):
    """
    Configura un logger con rotación de archivos y salida a consola segura (UTF-8).
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Evitar duplicar handlers si ya existen
    if logger.handlers:
        return logger
        
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 1. Rotating File Handler (Max 5MB, 3 backups)
    if log_file:
        file_path = os.path.join(LOG_DIR, log_file)
        file_handler = logging.handlers.RotatingFileHandler(
            file_path, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # 2. Console Handler (Safe for Windows)
    # En Windows, stdout a veces falla con caracteres unicode si no se configura bien.
    # Aquí asumimos que el entorno ya está sanitizado o usamos handlers estándar.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def configure_windows_console():
    """Configurar la consola de Windows para soportar UTF-8 si es necesario."""
    if sys.platform == 'win32' and sys.stdout.encoding.lower() != 'utf-8':
        try:
            import io
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        except:
            pass
