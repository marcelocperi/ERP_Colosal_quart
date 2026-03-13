
import asyncio
from hypercorn.config import Config
from hypercorn.asyncio import serve
from app import app
import logging
import os
import sys

# Configurar logging para producción
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("server_production_quart.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('hypercorn_server')

async def main():
    config = Config()
    port = int(os.environ.get('PORT', 5000))
    config.bind = [f"0.0.0.0:{port}"]
    config.use_reloader = False
    
    logger.info(f"Iniciando Servidor Hypercorn (Quart) en 0.0.0.0:{port}...")
    try:
        await serve(app, config)
    except Exception as e:
        logger.error(f"Error fatal al iniciar Hypercorn: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
