
import requests
import random
import time
from stem import Signal
from stem.control import Controller

class RotationManager:
    """
    Gestiona la rotación de identidades. Soporta Tor o Proxy Pool.
    """
    async def __init__(self, mode='DIRECT', rotation_threshold=10, api_key=None):
        self.mode = mode # 'TOR', 'POOL', 'SCRAPERAPI' o 'DIRECT'
        self.rotation_threshold = rotation_threshold
        self.api_key = api_key
        self.request_count = 0
        self.proxies_pool = [] # Lista de strings 'ip:port'
        self.current_proxy = None
        self.session = requests.Session()
        await self._initialize_session()

    async def _initialize_session(self):
        """Configura la sesión inicial según el modo."""
        self.session.proxies = {} # Reset
        
        if self.mode == 'DIRECT':
            return
            
        if self.mode == 'TOR':
            self.session.proxies = {
                'http': 'socks5h://127.0.0.1:9050',
                'https': 'socks5h://127.0.0.1:9050'
            }
        elif self.mode == 'POOL' and self.proxies_pool:
            self.current_proxy = random.choice(self.proxies_pool)
            self.session.proxies = {
                'http': f'http://{self.current_proxy}',
                'https': f'http://{self.current_proxy}'
            }
        
        # User-Agent rotation (siempre bueno)
        await self.await session.await headers.update({
            'User-Agent': random.choice([
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
            ])
        })

    async def set_proxies(self, proxy_list):
        """Asigna una lista de proxies manuales y cambia a modo POOL."""
        self.proxies_pool = proxy_list
        self.mode = 'POOL'
        await self._initialize_session()

    def rotate(self):
        """Rota la identidad (Nueva IP)."""
        if self.mode == 'TOR':
            try:
                with Controller.from_port(port=9051) as controller:
                    controller.authenticate() # Requiere Tor configurado con ControlPort
                    controller.signal(Signal.NEWNYM)
                print("Nodo Tor rotado exitosamente.")
            except Exception as e:
                print(f"Tor no disponible o bloqueado: {e}")
                # Si Tor falla, podríamos intentar cambiar a modo directo temporalmente
        elif self.mode == 'POOL' and self.proxies_pool:
            old_proxy = self.current_proxy
            while len(self.proxies_pool) > 1 and self.current_proxy == old_proxy:
                self.current_proxy = random.choice(self.proxies_pool)
            self.session.proxies = {
                'http': f'http://{self.current_proxy}',
                'https': f'http://{self.current_proxy}'
            }
            print(f"Proxy rotado a: {self.current_proxy}")
        
        self.request_count = 0

    def get(self, url, **kwargs):
        """Realiza la petición y cuenta para la rotación."""
        if self.mode == 'SCRAPERAPI' and self.api_key:
            # En modo ScraperAPI, redirigimos la URL a través de su gateway
            proxy_url = f"http://api.scraperapi.com?api_key={self.api_key}&url={url}"
            return self.session.get(proxy_url, **kwargs)

        self.request_count += 1
        if self.request_count > self.rotation_threshold:
            print(f"Rotación automática activada (cada {self.rotation_threshold} regs).")
            self.rotate()
            
        try:
            return self.session.get(url, **kwargs)
        except Exception as e:
            print(f"Error en petición ({self.mode}), intentando rotar: {e}")
            self.rotate()
            return self.session.get(url, **kwargs)

# Instancia global (Empieza en DIRECT por seguridad)
rotation_manager = RotationManager(mode='DIRECT')
