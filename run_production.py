
import os
import sys
import logging
from waitress import serve
from app import app

# Configurar logging para producción
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("server_production.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('production_server')

def start_server():
    # Determinamos el puerto (5000 por defecto para Colosal ERP)
    port = int(os.environ.get('PORT', 5000))
    host = '0.0.0.0'
    
    logger.info(f"Iniciando Servidor Waitress en {host}:{port}...")
    try:
        # Iniciamos el servicio
        serve(app, host=host, port=port, threads=8)
    except Exception as e:
        logger.error(f"Error fatal al iniciar el servidor: {e}")
        sys.exit(1)

if __name__ == '__main__':
    start_server()
