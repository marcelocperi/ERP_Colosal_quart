"""
Servicio para consumir la API pública del BCRA.

Entidades Bancarias (CBU):  https://api.bcra.gob.ar/financial-institutions/v1.0/entidades
Billeteras Virtuales (CVU): https://api.bcra.gob.ar/financial-institutions/v1.0/entidades
                             filtrando por tipo PSP / usando endpoint de proveedores

Lógica de cuentas contables analíticas
---------------------------------------
  CBU (banco tradicional):
      codigo   = primeros 3 dígitos del bcra_id   → zfill(3)
      cuenta   = 1.1.02.XXX   nombre = "Banco <nombre>"

  CVU (billetera virtual / PSP):
      codigo   = últimos 3 dígitos del numero_cuenta
      cuenta   = 1.1.03.XXX   nombre = "Billetera <nombre>"

Ejemplo:
  Mercado Libre  numero_cuenta = "0000003100000000000031"
  código         = "031"
  cuenta         = 1.1.03.031  / "Billetera Mercado Libre"
"""

import requests
import logging
from database import get_db_cursor

logger = logging.getLogger(__name__)

BCRA_BASE_URL = "https://api.bcra.gob.ar/financial-institutions/v1.0"

# Mapa bcra_id → codigo_cbu conocidos para la nomenclatura del CBU
BCRA_CBU_CODES = {
    11:  "011",   # Banco de la Nación Argentina
    14:  "014",   # Banco de la Provincia de Buenos Aires
    20:  "020",   # Banco de la Provincia de Córdoba
    29:  "029",   # Banco de la Ciudad de Buenos Aires
    17:  "017",   # BBVA Argentina
    72:  "072",   # Banco Santander Argentina
    7:   "007",   # Banco Galicia
    93:  "093",   # Banco ICBC
    15:  "015",   # Banco HSBC
    44:  "044",   # Banco Patagonia
    24:  "024",   # Banco Macro
    75:  "075",   # Banco Itaú Argentina
    16:  "016",   # Citibank
    8:   "008",   # Banco Credicoop Cooperativo
}

# PSP conocidos con sus CVU identificatorios (últimos 3 dígitos de numero_cuenta)
# Fuente: estructura de CVU BCRA — segmento entidad posiciones 8-10 del CVU
PSP_CVU_CODES = {
    "Mercado Pago":       "031",
    "Ualá":               "080",
    "Naranja X":          "015",
    "Brubank":            "143",
    "Personal Pay":       "278",
    "Modo":               "384",
    "Wibond":             "310",
    "Cuenta DNI":         "091",
    "Prex":               "261",
    "Lemon Cash":         "384",
}


class BCRAService:
    """
    Servicio de integración con la API de Entidades Financieras del BCRA.
    Gestiona bancos (CBU) y billeteras virtuales / PSP (CVU).
    """

    TIMEOUT = 15
    CODIGO_PADRE_BANCOS    = "1.1.02"   # Bancos CBU
    CODIGO_PADRE_BILLETERAS = "1.1.03"  # Billeteras CVU

    @classmethod
    def initialize_db(cls):
        """Crea la tabla fin_bancos si no existe (con todos los campos)."""
        with get_db_cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fin_bancos (
                    id                 INT AUTO_INCREMENT PRIMARY KEY,
                    enterprise_id      INT NOT NULL DEFAULT 0,
                    bcra_id            INT NULL,
                    tipo_entidad       ENUM('CBU','CVU') NOT NULL DEFAULT 'CBU',
                    numero_cuenta      VARCHAR(22) NULL
                                       COMMENT 'Número completo CBU/CVU (22 dígitos)',
                    codigo_cbu         CHAR(3) NULL
                                       COMMENT 'Código 3 dígitos: primeros del CBU o últimos del CVU',
                    cuenta_contable_id INT NULL,
                    nombre             VARCHAR(200) NOT NULL,
                    tipo               VARCHAR(100) NULL,
                    cuit               VARCHAR(15)  NULL,
                    bic                VARCHAR(20)  NULL,
                    direccion          VARCHAR(300) NULL,
                    telefono           VARCHAR(50)  NULL,
                    web                VARCHAR(200) NULL,
                    activo             TINYINT(1)   NOT NULL DEFAULT 1,
                    origen             ENUM('MANUAL','BCRA') NOT NULL DEFAULT 'MANUAL',
                    created_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at         DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_bcra_tipo (bcra_id, tipo_entidad),
                    INDEX idx_enterprise    (enterprise_id),
                    INDEX idx_nombre        (nombre(100)),
                    INDEX idx_activo        (activo),
                    INDEX idx_origen        (origen),
                    INDEX idx_tipo_entidad  (tipo_entidad)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
        logger.info("Tabla fin_bancos verificada/creada.")

    # ── Lógica de cuentas contables ──────────────────────────────────────────

    @classmethod
    def _codigo_cuenta_desde_numero(cls, tipo_entidad, bcra_id=None, numero_cuenta=None):
        """
        Devuelve el código de 3 dígitos para la cuenta analítica:
          CBU → primeros 3 del bcra_id  (o del mapa BCRA_CBU_CODES)
          CVU → últimos 3 del numero_cuenta
        """
        if tipo_entidad == 'CVU':
            if numero_cuenta and len(str(numero_cuenta).strip()) >= 3:
                return str(numero_cuenta).strip()[-3:].zfill(3)
            return None
        else:  # CBU
            if bcra_id is not None:
                mapped = BCRA_CBU_CODES.get(int(bcra_id))
                if mapped:
                    return mapped
                try:
                    n = int(bcra_id)
                    if 1 <= n <= 999:
                        return str(n).zfill(3)
                except (ValueError, TypeError):
                    pass
            return None

    @classmethod
    def _prefijo_cuenta(cls, tipo_entidad):
        """Retorna el código padre según el tipo: 1.1.02 (CBU) o 1.1.03 (CVU)."""
        return cls.CODIGO_PADRE_BILLETERAS if tipo_entidad == 'CVU' else cls.CODIGO_PADRE_BANCOS

    @classmethod
    def _nombre_cuenta(cls, tipo_entidad, nombre_entidad):
        """Arma el nombre de la cuenta analítica."""
        if tipo_entidad == 'CVU':
            return f"Billetera {nombre_entidad}"
        return f"Banco {nombre_entidad}"

    @classmethod
    def _get_o_crear_cuenta(cls, cursor, enterprise_id, tipo_entidad, codigo_3d, nombre_entidad):
        """
        Busca o crea la cuenta analítica en cont_plan_cuentas.

        CBU → 1.1.02.XXX  / "Banco BBVA"
        CVU → 1.1.03.XXX  / "Billetera Mercado Libre"

        Retorna el id de cont_plan_cuentas o None.
        """
        if not codigo_3d:
            return None

        codigo_3d    = str(codigo_3d).zfill(3)
        prefijo      = cls._prefijo_cuenta(tipo_entidad)
        codigo_cta   = f"{prefijo}.{codigo_3d}"
        nombre_cta   = cls._nombre_cuenta(tipo_entidad, nombre_entidad)

        # ¿Ya existe?
        cursor.execute("""
            SELECT id FROM cont_plan_cuentas
            WHERE codigo = %s AND (enterprise_id = %s OR enterprise_id = 0)
            LIMIT 1
        """, (codigo_cta, enterprise_id))
        row = cursor.fetchone()
        if row:
            cursor.execute("UPDATE cont_plan_cuentas SET nombre = %s WHERE id = %s",
                           (nombre_cta, row[0]))
            return row[0]

        # Buscar cuenta padre (1.1.02 o 1.1.03)
        cursor.execute("""
            SELECT id FROM cont_plan_cuentas
            WHERE codigo = %s AND (enterprise_id = %s OR enterprise_id = 0)
            LIMIT 1
        """, (prefijo, enterprise_id))
        padre_row = cursor.fetchone()
        padre_id  = padre_row[0] if padre_row else None

        cursor.execute("""
            INSERT INTO cont_plan_cuentas
                (enterprise_id, codigo, nombre, tipo, imputable, padre_id, nivel, es_analitica)
            VALUES (%s, %s, %s, 'ACTIVO', 1, %s, 4, 1)
        """, (enterprise_id, codigo_cta, nombre_cta, padre_id))

        cta_id = cursor.lastrowid
        logger.info(f"Cuenta creada: {codigo_cta} - {nombre_cta} (id={cta_id})")
        return cta_id

    # ── API del BCRA ─────────────────────────────────────────────────────────

    @classmethod
    def _get_json_bcra(cls, url):
        """Realiza GET a la API del BCRA y retorna la lista de resultados."""
        headers = {"Accept": "application/json", "User-Agent": "BibliotecaWEB-ERP/1.0"}
        resp = requests.get(url, headers=headers, timeout=cls.TIMEOUT, verify=True)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("results", data.get("entidades", data.get("data", [])))

    @classmethod
    def get_entidades_bcra(cls):
        """Descarga entidades bancarias (CBU) del BCRA."""
        try:
            return cls._get_json_bcra(f"{BCRA_BASE_URL}/entidades")
        except requests.exceptions.ConnectionError:
            raise ConnectionError("No se pudo conectar con la API del BCRA.")
        except requests.exceptions.Timeout:
            raise TimeoutError(f"Timeout ({cls.TIMEOUT}s) conectando con el BCRA.")
        except requests.exceptions.HTTPError as e:
            raise ValueError(f"HTTP {e.response.status_code}: {e.response.text[:200]}")

    @classmethod
    def get_billeteras_bcra(cls):
        """
        Descarga billeteras virtuales / PSP (CVU) del BCRA.
        Intenta varios endpoints conocidos de la API.
        """
        endpoints = [
            f"{BCRA_BASE_URL}/entidades?tipo=PSP",
            f"{BCRA_BASE_URL}/billeteras",
            f"{BCRA_BASE_URL}/proveedores-servicios-pago",
        ]
        for url in endpoints:
            try:
                results = cls._get_json_bcra(url)
                if results:
                    logger.info(f"Billeteras BCRA obtenidas desde: {url} ({len(results)} registros)")
                    return results
            except Exception as e:
                logger.debug(f"Endpoint {url} no disponible: {e}")

        # Fallback: datos semilla de PSP conocidos
        logger.warning("API BCRA no devolvió billeteras. Usando listado semilla.")
        return cls._seed_billeteras()

    @classmethod
    def _seed_billeteras(cls):
        """Listado semilla de billeteras virtuales argentinas con sus CVU identificatorios."""
        return [
            {"denominacion": "Mercado Pago",   "tipoEntidad": "PSP", "numeroCVU": "0000003100000000000031"},
            {"denominacion": "Ualá",            "tipoEntidad": "PSP", "numeroCVU": "0000008000000000000080"},
            {"denominacion": "Naranja X",       "tipoEntidad": "PSP", "numeroCVU": "0000001500000000000015"},
            {"denominacion": "Brubank",         "tipoEntidad": "PSP", "numeroCVU": "0000014300000000000143"},
            {"denominacion": "Personal Pay",    "tipoEntidad": "PSP", "numeroCVU": "0000027800000000000278"},
            {"denominacion": "Modo",            "tipoEntidad": "PSP", "numeroCVU": "0000038400000000000384"},
            {"denominacion": "Cuenta DNI",      "tipoEntidad": "PSP", "numeroCVU": "0000009100000000000091"},
            {"denominacion": "Prex",            "tipoEntidad": "PSP", "numeroCVU": "0000026100000000000261"},
            {"denominacion": "Lemon Cash",      "tipoEntidad": "PSP", "numeroCVU": "0000038500000000000385"},
            {"denominacion": "Wibond",          "tipoEntidad": "PSP", "numeroCVU": "0000031000000000000310"},
        ]

    # ── Sincronización ────────────────────────────────────────────────────────

    @classmethod
    def _upsert_entidad(cls, cursor, enterprise_id, tipo_entidad,
                        bcra_id, nombre, tipo,
                        numero_cuenta=None, direccion=None, telefono=None,
                        web=None, cuit=None, bic=None):
        """
        Inserta o actualiza una entidad en fin_bancos.
        
        Regla de actualización (nunca elimina datos ya existentes):
          - nombre / tipo : siempre se actualiza desde la API
          - direccion / telefono / web / cuit / bic:
              solo actualiza si la API trae un valor NO nulo.
              Si la API no envía el campo, se conserva el valor actual.
        """
        codigo_3d = cls._codigo_cuenta_desde_numero(
            tipo_entidad, bcra_id=bcra_id, numero_cuenta=numero_cuenta)

        # Requerimiento: para CVU, el bcra_id debe ser los 3 últimos dígitos (numérico)
        if tipo_entidad == 'CVU' and codigo_3d:
            try:
                bcra_id = int(codigo_3d)
            except ValueError:
                pass

        cuenta_id = cls._get_o_crear_cuenta(
            cursor, enterprise_id, tipo_entidad, codigo_3d, nombre) if codigo_3d else None

        cursor.execute("""
            INSERT INTO fin_bancos
                (enterprise_id, bcra_id, tipo_entidad, numero_cuenta, codigo_cbu,
                 cuenta_contable_id, nombre, tipo, cuit, bic,
                 direccion, telefono, web, origen, activo)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, 'BCRA', 1)
            ON DUPLICATE KEY UPDATE
                nombre             = VALUES(nombre),
                tipo               = VALUES(tipo),
                tipo_entidad       = VALUES(tipo_entidad),
                numero_cuenta      = COALESCE(VALUES(numero_cuenta),  numero_cuenta),
                codigo_cbu         = COALESCE(VALUES(codigo_cbu),     codigo_cbu),
                cuenta_contable_id = COALESCE(VALUES(cuenta_contable_id), cuenta_contable_id),
                cuit      = IF(VALUES(cuit)      IS NOT NULL AND VALUES(cuit)      != '', VALUES(cuit),      cuit),
                bic       = IF(VALUES(bic)       IS NOT NULL AND VALUES(bic)       != '', VALUES(bic),       bic),
                direccion = IF(VALUES(direccion) IS NOT NULL AND VALUES(direccion) != '', VALUES(direccion), direccion),
                telefono  = IF(VALUES(telefono)  IS NOT NULL AND VALUES(telefono)  != '', VALUES(telefono),  telefono),
                web       = IF(VALUES(web)       IS NOT NULL AND VALUES(web)       != '', VALUES(web),       web),
                activo    = 1,
                updated_at = CURRENT_TIMESTAMP
        """, (enterprise_id, bcra_id, tipo_entidad, numero_cuenta, codigo_3d,
               cuenta_id, nombre, tipo, cuit, bic,
               direccion, telefono, web))

        return cursor.rowcount, cuenta_id

    @classmethod
    def sincronizar_desde_bcra(cls, enterprise_id=0):
        """Sincroniza bancos (CBU) desde la API del BCRA."""
        cls.initialize_db()
        entidades = cls.get_entidades_bcra()
        stats = {"insertados": 0, "actualizados": 0, "cuentas": 0, "errores": 0,
                 "total": len(entidades), "tipo": "CBU"}

        with get_db_cursor() as cursor:
            for ent in entidades:
                try:
                    bcra_id   = ent.get("cdEntidad") or ent.get("id") or ent.get("codigo")
                    nombre    = (ent.get("denominacion") or ent.get("nombre") or "").strip()
                    tipo      = ent.get("tipoEntidad") or ent.get("tipo") or ""
                    # Campos de contacto que puede traer la API
                    direccion = (ent.get("domicilio") or ent.get("direccion") or "").strip() or None
                    telefono  = (ent.get("telefono") or ent.get("phone") or "").strip() or None
                    web       = (ent.get("web") or ent.get("url") or "").strip() or None
                    cuit      = (ent.get("cuit") or ent.get("cuil") or "").strip() or None
                    bic       = (ent.get("bic") or ent.get("swift") or "").strip() or None

                    if not nombre:
                        stats["errores"] += 1
                        continue

                    rc, cuenta_id = cls._upsert_entidad(
                        cursor, enterprise_id, 'CBU', bcra_id, nombre, tipo,
                        direccion=direccion, telefono=telefono, web=web,
                        cuit=cuit, bic=bic)
                    if rc == 1:
                        stats["insertados"] += 1
                    else:
                        stats["actualizados"] += 1
                    if cuenta_id:
                        stats["cuentas"] += 1
                except Exception as e:
                    logger.warning(f"Error entidad BCRA {ent}: {e}")
                    stats["errores"] += 1

        logger.info(f"Sync CBU: {stats}")
        return stats

    @classmethod
    def sincronizar_billeteras(cls, enterprise_id=0):
        """Sincroniza billeteras virtuales (CVU) desde la API del BCRA o listado semilla."""
        cls.initialize_db()
        billeteras = cls.get_billeteras_bcra()
        stats = {"insertados": 0, "actualizados": 0, "cuentas": 0, "errores": 0,
                 "total": len(billeteras), "tipo": "CVU"}

        with get_db_cursor() as cursor:
            for b in billeteras:
                try:
                    bcra_id   = b.get("cdEntidad") or b.get("id") or b.get("codigo")
                    nombre    = (b.get("denominacion") or b.get("nombre") or "").strip()
                    tipo      = b.get("tipoEntidad") or "PSP"
                    num_cvu   = (b.get("numeroCVU") or b.get("cvu") or
                                 b.get("numero_cuenta") or "").strip() or None
                    direccion = (b.get("domicilio") or b.get("direccion") or "").strip() or None
                    telefono  = (b.get("telefono") or b.get("phone") or "").strip() or None
                    web       = (b.get("web") or b.get("url") or "").strip() or None
                    cuit      = (b.get("cuit") or b.get("cuil") or "").strip() or None

                    if not nombre:
                        stats["errores"] += 1
                        continue

                    rc, cuenta_id = cls._upsert_entidad(
                        cursor, enterprise_id, 'CVU', bcra_id, nombre, tipo,
                        numero_cuenta=num_cvu, direccion=direccion,
                        telefono=telefono, web=web, cuit=cuit)
                    if rc == 1:
                        stats["insertados"] += 1
                    else:
                        stats["actualizados"] += 1
                    if cuenta_id:
                        stats["cuentas"] += 1
                except Exception as e:
                    logger.warning(f"Error billetera {b}: {e}")
                    stats["errores"] += 1

        logger.info(f"Sync CVU: {stats}")
        return stats

    # ── Helpers ───────────────────────────────────────────────────────────────

    @classmethod
    def crear_cuenta_para_banco(cls, cursor, enterprise_id, banco_id, nombre, tipo_entidad,
                                bcra_id=None, numero_cuenta=None):
        """
        Crea/obtiene la cuenta analítica para una entidad y actualiza fin_bancos.
        Útil al dar de alta manualmente.
        """
        codigo_3d = cls._codigo_cuenta_desde_numero(
            tipo_entidad, bcra_id=bcra_id, numero_cuenta=numero_cuenta)
        if not codigo_3d:
            return None

        # Requerimiento: para CVU, el bcra_id numerico con los 3 ultimos
        if tipo_entidad == 'CVU':
            try: bcra_id = int(codigo_3d)
            except ValueError: pass

        cuenta_id = cls._get_o_crear_cuenta(
            cursor, enterprise_id, tipo_entidad, codigo_3d, nombre)
        if cuenta_id and banco_id:
            cursor.execute("""
                UPDATE fin_bancos
                SET cuenta_contable_id = %s, codigo_cbu = %s, bcra_id = COALESCE(%s, bcra_id)
                WHERE id = %s
            """, (cuenta_id, codigo_3d, bcra_id, banco_id))
        return cuenta_id

    @staticmethod
    def get_bancos_db(enterprise_id, solo_activos=False, tipo_entidad=None):
        """Retorna bancos/billeteras de la base local con su cuenta contable."""
        with get_db_cursor(dictionary=True) as cursor:
            sql = """
                SELECT b.*,
                       c.codigo AS cuenta_codigo,
                       c.nombre AS cuenta_nombre
                FROM fin_bancos b
                LEFT JOIN cont_plan_cuentas c ON b.cuenta_contable_id = c.id
                WHERE (b.enterprise_id = %s OR b.enterprise_id = 0)
            """
            params = [enterprise_id]
            if solo_activos:
                sql += " AND b.activo = 1"
            if tipo_entidad:
                sql += " AND b.tipo_entidad = %s"
                params.append(tipo_entidad)
            sql += " ORDER BY b.tipo_entidad, b.nombre ASC"
            cursor.execute(sql, params)
            return cursor.fetchall()


# ==============================================================================
# SERVICIO DE TIPOS DE CAMBIO — BCRA API Pública
# ==============================================================================
# Endpoint oficial: https://api.bcra.gob.ar/estadisticas/v2.0/datosvariable/{id}/{desde}/{hasta}
# Variables clave:
#   1  → Tipo de cambio de referencia (vendedor) — USD Oficial
#   4  → Tipo de cambio minorista — USD Minorista BCRA
#   5  → Tipo de pase activo (LECAPs) — referencia MEP
#
# Para EUR/BRL/otros se usa el endpoint de cotizaciones de divisas:
# https://api.bcra.gob.ar/estadisticas/v2.0/divisas
# ==============================================================================

BCRA_STATS_URL = "https://api.bcra.gob.ar/estadisticas/v2.0"

# Mapa: (variable_id, tipo_etiqueta) — Variables de referencia del BCRA
BCRA_VARIABLES_TC = {
    1:  "OFICIAL_VENDEDOR",    # Tipo de cambio de referencia (mayorista)
    4:  "OFICIAL_MINORISTA",   # Tipo de cambio minorista
}

# Código ISO BCRA → código ISO estándar
BCRA_DIVISAS_MAPA = {
    "2222": "USD",  # Dólar EEUU
    "2230": "EUR",  # Euro
    "2280": "BRL",  # Real Brasileño
    "2236": "GBP",  # Libra Esterlina
    "2276": "JPY",  # Yen Japonés
    "2240": "CHF",  # Franco Suizo
    "2256": "CNY",  # Yuan Chino
}


class CurrencyRateService:
    """
    Servicio para obtener y almacenar tipos de cambio desde la API pública del BCRA.
    
    Uso típico (desde cron job):
        CurrencyRateService.actualizar_cotizaciones_hoy()
    
    Uso en formularios de importación:
        tc = CurrencyRateService.get_tipo_cambio('USD', tipo='OFICIAL_VENDEDOR')
    """

    TIMEOUT = 15
    FUENTE  = "BCRA"

    @classmethod
    def _get_bcra(cls, url):
        """GET a la API del BCRA, retorna lista de resultados."""
        headers = {"Accept": "application/json", "User-Agent": "BibliotecaWEB-ERP/1.0"}
        try:
            resp = requests.get(url, headers=headers, timeout=cls.TIMEOUT, verify=True)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return data.get("results", data.get("data", []))
        except requests.exceptions.RequestException as e:
            logger.warning(f"[CurrencyRate] Error BCRA: {e}")
            return []

    @classmethod
    def _upsert_tipo_cambio(cls, cursor, enterprise_id, fecha, moneda, tipo, valor, fuente="BCRA"):
        """Inserta o actualiza un tipo de cambio en fin_tipos_cambio."""
        cursor.execute("""
            INSERT INTO fin_tipos_cambio (enterprise_id, fecha, moneda, tipo, valor, fuente)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE valor = VALUES(valor), fuente = VALUES(fuente)
        """, (enterprise_id, fecha, moneda, tipo, valor, fuente))

    @classmethod
    def actualizar_cotizaciones_hoy(cls, enterprise_id=0):
        """
        Descarga los tipos de cambio del día desde el BCRA y los guarda en fin_tipos_cambio.
        Diseñado para ejecutarse desde el cron job diario.
        Retorna un dict con estadísticas de la operación.
        """
        import datetime as dt
        hoy      = dt.date.today().isoformat()
        ayer     = (dt.date.today() - dt.timedelta(days=3)).isoformat()  # margen para fines de semana
        stats    = {"actualizados": 0, "errores": 0, "monedas": []}

        with get_db_cursor() as cursor:
            # ---------- 1. USD Oficial (variable 1 = mayorista referencia) ----------
            try:
                data = cls._get_bcra(f"{BCRA_STATS_URL}/datosvariable/1/{ayer}/{hoy}")
                if data:
                    ultimo = sorted(data, key=lambda x: x.get("fecha", ""))[-1]
                    valor  = float(ultimo.get("valor", 0))
                    fecha  = ultimo.get("fecha", hoy)[:10]
                    if valor > 0:
                        cls._upsert_tipo_cambio(cursor, enterprise_id, fecha, "USD", "OFICIAL_VENDEDOR", valor)
                        stats["actualizados"] += 1
                        stats["monedas"].append(f"USD/OFICIAL={valor}")
            except Exception as e:
                logger.warning(f"[CurrencyRate] Error USD variable 1: {e}")
                stats["errores"] += 1

            # ---------- 2. USD Minorista (variable 4) ----------
            try:
                data = cls._get_bcra(f"{BCRA_STATS_URL}/datosvariable/4/{ayer}/{hoy}")
                if data:
                    ultimo = sorted(data, key=lambda x: x.get("fecha", ""))[-1]
                    valor  = float(ultimo.get("valor", 0))
                    fecha  = ultimo.get("fecha", hoy)[:10]
                    if valor > 0:
                        cls._upsert_tipo_cambio(cursor, enterprise_id, fecha, "USD", "OFICIAL_MINORISTA", valor)
                        stats["actualizados"] += 1
                        stats["monedas"].append(f"USD/MINORISTA={valor}")
            except Exception as e:
                logger.warning(f"[CurrencyRate] Error USD variable 4: {e}")
                stats["errores"] += 1

            # ---------- 3. Divisas múltiples (EUR, BRL, GBP, etc.) ----------
            try:
                divisas = cls._get_bcra(f"{BCRA_STATS_URL}/divisas")
                for d in divisas:
                    codigo_bcra = str(d.get("codigo", ""))
                    moneda_iso  = BCRA_DIVISAS_MAPA.get(codigo_bcra)
                    if not moneda_iso:
                        continue  # Solo las que tenemos mapeadas
                    valor = float(d.get("tipoPase", d.get("cotizacion", d.get("valor", 0))) or 0)
                    if valor > 0:
                        cls._upsert_tipo_cambio(cursor, enterprise_id, hoy, moneda_iso, "OFICIAL_VENDEDOR", valor)
                        stats["actualizados"] += 1
                        stats["monedas"].append(f"{moneda_iso}={valor}")
            except Exception as e:
                logger.warning(f"[CurrencyRate] Error divisas: {e}")
                stats["errores"] += 1

        logger.info(f"[CurrencyRate] Actualización completada: {stats}")
        return stats

    @classmethod
    def get_tipo_cambio(cls, moneda="USD", tipo="OFICIAL_VENDEDOR", fecha=None, enterprise_id=0):
        """
        Retorna el tipo de cambio más reciente (o de la fecha indicada) para la moneda/tipo.
        
        Prioridad: fecha exacta → última fecha disponible.
        Si no hay datos de BD, intenta un fallback a la API en tiempo real.
        
        Retorna: float (valor en ARS por 1 unidad de moneda extranjera) o None.
        """
        import datetime as dt
        if not fecha:
            fecha = dt.date.today().isoformat()
        
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Buscar exactamente la fecha
            cursor.execute("""
                SELECT valor FROM fin_tipos_cambio 
                WHERE (enterprise_id = %s OR enterprise_id = 0)
                  AND moneda = %s AND tipo = %s AND fecha = %s
                ORDER BY enterprise_id DESC LIMIT 1
            """, (enterprise_id, moneda, tipo, fecha))
            row = cursor.fetchone()
            if row:
                return float(row["valor"])
            
            # 2. Buscar el más reciente antes de esa fecha
            cursor.execute("""
                SELECT valor FROM fin_tipos_cambio 
                WHERE (enterprise_id = %s OR enterprise_id = 0)
                  AND moneda = %s AND tipo = %s AND fecha <= %s
                ORDER BY fecha DESC, enterprise_id DESC LIMIT 1
            """, (enterprise_id, moneda, tipo, fecha))
            row = cursor.fetchone()
            if row:
                return float(row["valor"])

        # 3. Fallback: intentar actualizar y reintentar
        logger.info(f"[CurrencyRate] Sin datos para {moneda}/{tipo}/{fecha}. Intentando actualizar...")
        try:
            cls.actualizar_cotizaciones_hoy(enterprise_id)
            with get_db_cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT valor FROM fin_tipos_cambio 
                    WHERE (enterprise_id = %s OR enterprise_id = 0)
                      AND moneda = %s AND tipo = %s
                    ORDER BY fecha DESC LIMIT 1
                """, (enterprise_id, moneda, tipo))
                row = cursor.fetchone()
                if row:
                    return float(row["valor"])
        except Exception as e:
            logger.error(f"[CurrencyRate] Fallback falló: {e}")
        
        return None

    @classmethod
    def get_all_vigentes(cls, enterprise_id=0):
        """
        Retorna todos los tipos de cambio vigentes (última fecha por moneda/tipo).
        Útil para poblar selectores en formularios de importación.
        """
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT t1.*
                FROM fin_tipos_cambio t1
                INNER JOIN (
                    SELECT moneda, tipo, MAX(fecha) as max_fecha
                    FROM fin_tipos_cambio
                    WHERE enterprise_id IN (0, %s)
                    GROUP BY moneda, tipo
                ) t2 ON t1.moneda = t2.moneda AND t1.tipo = t2.tipo AND t1.fecha = t2.max_fecha
                WHERE t1.enterprise_id IN (0, %s)
                ORDER BY t1.moneda, t1.tipo
            """, (enterprise_id, enterprise_id))
            return cursor.fetchall()

    @classmethod
    def registrar_manual(cls, enterprise_id, moneda, tipo, valor, fecha=None, user_id=None):
        """
        Permite registrar un tipo de cambio manualmente (cuando la API no está disponible
        o el usuario necesita usar un TC específico acordado en el contrato).
        """
        import datetime as dt
        if not fecha:
            fecha = dt.date.today().isoformat()
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO fin_tipos_cambio (enterprise_id, fecha, moneda, tipo, valor, fuente, user_id)
                VALUES (%s, %s, %s, %s, %s, 'MANUAL', %s)
                ON DUPLICATE KEY UPDATE valor = VALUES(valor), fuente = 'MANUAL', user_id = VALUES(user_id)
            """, (enterprise_id, fecha, moneda, tipo, valor, user_id))
        logger.info(f"[CurrencyRate] Manual: {moneda}/{tipo}={valor} para {fecha} (ent={enterprise_id})")
        return True
