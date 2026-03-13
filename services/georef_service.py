
import requests
import logging
from database import get_db_cursor

logger = logging.getLogger(__name__)

class GeorefService:
    API_PROVINCIAS = "https://apis.datos.gob.ar/georef/api/v2.0/provincias"
    API_LOCALIDADES = "https://apis.datos.gob.ar/georef/api/v2.0/localidades"
    API_CALLES = "https://apis.datos.gob.ar/georef/api/v2.0/calles"

    @staticmethod
    async def initialize_db():
        """Crea las tablas de geografía local."""
        async with get_db_cursor() as cursor:
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS sys_provincias (
                    id VARCHAR(10) PRIMARY KEY,
                    nombre VARCHAR(100) NOT NULL,
                    iso_id VARCHAR(10),
                    centroide_lat DECIMAL(15, 12),
                    centroide_lon DECIMAL(15, 12)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS sys_localidades (
                    id VARCHAR(20) PRIMARY KEY,
                    nombre VARCHAR(200) NOT NULL,
                    provincia_id VARCHAR(10) NOT NULL,
                    centroide_lat DECIMAL(15, 12),
                    centroide_lon DECIMAL(15, 12),
                    FOREIGN KEY (provincia_id) REFERENCES sys_provincias(id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS sys_calles (
                    id VARCHAR(20) PRIMARY KEY,
                    nombre VARCHAR(255) NOT NULL,
                    localidad_id VARCHAR(20),
                    provincia_id VARCHAR(10),
                    INDEX idx_localidad (localidad_id),
                    INDEX idx_provincia (provincia_id),
                    INDEX idx_nombre (nombre(50))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
        logger.info("Tablas de Georef local verificadas/creadas.")

    @classmethod
    async def load_provincias(cls):
        try:
            await cls.initialize_db()
            resp = requests.get(cls.API_PROVINCIAS, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            count = 0
            async with get_db_cursor() as cursor:
                for p in data.get('provincias', []):
                    await cursor.execute("""
                        INSERT INTO sys_provincias (id, nombre, iso_id, centroide_lat, centroide_lon)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE nombre=VALUES(nombre), iso_id=VALUES(iso_id)
                    """, (p['id'], p['nombre'], p.get('iso_id'), p['centroide']['lat'], p['centroide']['lon']))
                    count += 1
            return count
        except Exception as e:
            logger.error(f"Sync Provincias Error: {e}")
            return -1

    @classmethod
    async def load_localidades(cls):
        try:
            total = 0
            # Traer todas las localidades de una (aprox 4000)
            resp = requests.get(f"{cls.API_LOCALIDADES}?max=5000", timeout=30)
            resp.raise_for_status()
            data = resp.json()
            async with get_db_cursor() as cursor:
                for l in data.get('localidades', []):
                    await cursor.execute("""
                        INSERT INTO sys_localidades (id, nombre, provincia_id, centroide_lat, centroide_lon)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE nombre=VALUES(nombre), provincia_id=VALUES(provincia_id)
                    """, (l['id'], l['nombre'], l['provincia']['id'], l['centroide']['lat'], l['centroide']['lon']))
                    total += 1
            return total
        except Exception as e:
            logger.error(f"Sync Localidades Error: {e}")
            return -1

    @classmethod
    async def load_calles_by_provincia(cls, provincia_id):
        """Descarga e inserta calles para una provincia específica."""
        try:
            total = 0
            # Georef permite max=5000. Pero hay muchas calles.
            # Iteramos por provincia.
            resp = requests.get(f"{cls.API_CALLES}?provincia={provincia_id}&max=5000", timeout=60)
            resp.raise_for_status()
            data = resp.json()
            async with get_db_cursor() as cursor:
                for c in data.get('calles', []):
                    await cursor.execute("""
                        INSERT IGNORE INTO sys_calles (id, nombre, localidad_id, provincia_id)
                        VALUES (%s, %s, %s, %s)
                    """, (c['id'], c['nombre'], c.get('localidad', {}).get('id'), c['provincia']['id']))
                    total += 1
            return total
        except Exception as e:
            logger.error(f"Sync Calles Error (Prov {provincia_id}): {e}")
            return 0

    @classmethod
    async def sync_full(cls):
        """Orquestador para el cron job."""
        logger.info("Iniciando Sincronización Completa de Georef...")
        await cls.initialize_db()
        p_count = await cls.load_provincias()
        l_count = await cls.load_localidades()
        
        logger.info(f"Provincias: {p_count}, Localidades: {l_count}")
        
        # Para calles, lo ideal es iterar por provincias para no saturar la API
        total_calles = 0
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("SELECT id FROM sys_provincias")
            provincias = await cursor.fetchall()
            for p in provincias:
                logger.info(f"Sincronizando calles para provincia {p['id']}...")
                total_calles += await cls.load_calles_by_provincia(p['id'])
        
        logger.info(f"Sincronización finalizada. {total_calles} calles procesadas.")
        return True

    # --- MÉTODOS DE BÚSQUEDA REDIRIGIDOS A TABLAS LOCALES ---

    @staticmethod
    async def get_provincias():
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("SELECT * FROM sys_provincias ORDER BY nombre ASC")
            return await cursor.fetchall()

    @classmethod
    async def get_localidades(cls, provincia_nombre):
        try:
            async with get_db_cursor(dictionary=True) as cursor:
                query = """
                    SELECT DISTINCT l.nombre FROM sys_localidades l
                    JOIN sys_provincias p ON l.provincia_id = p.id
                    WHERE p.nombre LIKE %s
                    ORDER BY l.nombre ASC
                """
                await cursor.execute(query, (f"%{provincia_nombre}%",))
                return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Local search error (localidades): {e}")
            return []

    @classmethod
    async def get_calles(cls, localidad_nombre, provincia_nombre=None, nombre=None):
        """Búsqueda de calles REDIRIGIDA a tablas locales para evitar interoperabilidad de red."""
        if not nombre or len(nombre) < 3:
            return []
        
        try:
            async with get_db_cursor(dictionary=True) as cursor:
                # Búsqueda difusa en tabla local
                sql = """
                    SELECT DISTINCT c.nombre FROM sys_calles c
                    JOIN sys_provincias p ON c.provincia_id = p.id
                    JOIN sys_localidades l ON c.localidad_id = l.id
                    WHERE c.nombre LIKE %s
                """
                params = [f"%{nombre}%"]
                
                if localidad_nombre:
                    sql += " AND l.nombre LIKE %s"
                    params.append(f"%{localidad_nombre}%")
                
                if provincia_nombre:
                    sql += " AND p.nombre LIKE %s"
                    params.append(f"%{provincia_nombre}%")
                
                sql += " LIMIT 50"
                await cursor.execute(sql, params)
                return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Local search error (calles): {e}")
            # Si falla la búsqueda local (ej: tabla vacía), fallback a API? No, el usuario dijo "eliminar interoperatibilidad".
            return []

    @staticmethod
    async def get_cp_by_location(provincia_nombre, localidad_nombre):
        # Mantenemos la lógica de CP que ya usaba DB local + Heurística
        CP_BASE_MAP = {
            "Ciudad Autónoma de Buenos Aires": "1000", "Buenos Aires": "1900", "Catamarca": "4700",
            "Chaco": "3500", "Chubut": "9100", "Córdoba": "5000", "Corrientes": "3400",
            "Entre Ríos": "3100", "Formosa": "3600", "Jujuy": "4600", "La Pampa": "6300",
            "La Rioja": "5300", "Mendoza": "5500", "Misiones": "3300", "Neuquén": "8300",
            "Río Negro": "8500", "Salta": "4400", "San Juan": "5400", "San Luis": "5700",
            "Santa Cruz": "9400", "Santa Fe": "3000", "Santiago del Estero": "4200",
            "Tierra del Fuego, Antártida e Islas del Atlántico Sur": "9410", "Tucumán": "4000"
        }
        try:
            async with get_db_cursor(dictionary=True) as cursor:
                query = "SELECT DISTINCT cod_postal FROM erp_direcciones WHERE provincia LIKE %s AND localidad LIKE %s LIMIT 1"
                await cursor.execute(query, (f"%{provincia_nombre}%", f"%{localidad_nombre}%"))
                res = await cursor.fetchone()
                if res: return [res['cod_postal']]
        except: pass
        
        for k, v in CP_BASE_MAP.items():
            if k.lower() in provincia_nombre.lower(): return [v]
        return []
