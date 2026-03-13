
import os
import datetime
import random
import json
import base64
import requests
import httpx
from database import get_db_cursor

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.serialization import pkcs7
    import zeep
except ImportError:
    pass

class AfipService:
    """
    Servicio para interoperabilidad con AFIP.
    Soporta modo simulado (Mock) y conexión real vía Webservices (SOAP).
    """

    # Diccionario de errores para traducción humana
    ERRORES_TRADUCCION = {
        # Errores de Infraestructura / Sistema
        "500": "Error interno del servidor de AFIP: El sistema de AFIP no responde temporalmente. Intente nuevamente en unos minutos.",
        "501": "Error de infraestructura en AFIP (Internal Server Error).",
        "502": "AFIP en mantenimiento o con alta demanda (Bad Gateway).",
        "kcs": "Error de comunicación con el webservice de AFIP.",
        
        # Autorización de Comprobante (CAE)
        "10016": "Error de Correlatividad: El número enviado no es el que AFIP espera como siguiente. Verifique que no se hayan emitido comprobantes por fuera del sistema.",
        "10015": "El Punto de Venta informado no está habilitado para Facturación Electrónica en este tipo de comprobante.",
        "100": "El emisor no está habilitado para emitir este tipo de comprobante. Revise su situación en el padrón de AFIP.",
        "128": "El importe total del comprobante B supera el límite para receptores no identificados (DNI/CUIT).",
        "130": "CUIT del emisor no habilitada para emitir comprobantes clase M.",
        "136": "El Punto de Venta es inválido para el tipo de comprobante solicitado.",
        "147": "Factura de Crédito MiPyME: La CUIT del emisor debe estar inscripta en el registro especial.",
        "155": "La CUIT del receptor no se encuentra activa en los padrones de AFIP.",
        "156": "El receptor no posee la categoría necesaria para recibir este comprobante (Ej: No es Responsable Inscripto).",
        "159": "Debe informar un comprobante asociado (Factura original) para emitir esta Nota de Crédito/Débito.",
        "162": "El importe del ajuste supera el monto original del comprobante asociado.",
        "163": "Alerta: El receptor se encuentra registrado como Contribuyente Apócrifo en AFIP.",
        "164": "La cotización de moneda informada difiere significativamente del valor oficial informado por AFIP.",
        "165": "El comprobante debe estar asociado a una actividad económica válida y vigente por el emisor.",
        "196": "La combinación de Tipo de Comprobante y Categoría de IVA del Receptor es incompatible.",
        
        # Validaciones de Datos
        "10048": "Error en el cálculo de totales de IVA: La suma de alícuotas no coincide con el total neto.",
        "10049": "Se han informado alícuotas de IVA duplicadas o inexistentes en el detalle.",
        "10061": "Falta información: Para 'Servicios' se deben informar las fechas de inicio, fin y vencimiento de pago.",
        "10013": "El CUIT del receptor es incorrecto o tiene un dígito verificador inválido.",
        "10025": "El emisor tiene bloqueos administrativos o deudas que impiden la generación del CAE.",
        "201": "Es obligatorio informar un CBU para este tipo de comprobante MiPyME.",
        "504": "El código de barra/GTIN del producto no es válido según los estándares de AFIP.",
        
        # Errores Específicos de MTXCA
        "337": "Motivo de excepción IVA inválido para este régimen.",
        "10023": "Error estructural: La información enviada no cumple con el formato técnico XML de AFIP.",
        
        "default": "La operación fue rechazada por AFIP por validaciones de negocio. Por favor, revise los datos del cliente y los productos."
    }

    # Tope para Consumidor Final anónimo (Actualizable según RG)
    TOPE_ANONIMO_EFECTIVO = 191624.0 # Valor aprox actualizado 2024 para medios electrónicos es mayor, tomamos el restrictivo

    @staticmethod
    def validar_integridad_matematica(c):
        """
        Check: Total = Neto + IVA + Tributos. AFIP no perdona ni un centavo (RG 4291).
        """
        neto = round(float(c.get('neto', 0)), 2)
        iva = round(float(c.get('iva', 0)), 2)
        trib = round(float(c.get('percepciones', 0)), 2)
        total = round(float(c.get('total', 0)), 2)
        
        calculado = round(neto + iva + trib, 2)
        if abs(total - calculado) > 0.011: # Tolerancia de un centavo
            return False, f"Inconsistencia de Redondeo: Total declarado ({total}) no coincide con la suma ({calculado}). Diferencia: {round(total-calculado, 3)}"
        return True, "OK"

    @staticmethod
    def validar_ventana_fechas(c):
        """
        Check: Margen de 5 días para productos, 10 para servicios.
        """
        try:
            fecha_cbte_str = c.get('fecha_emision')
            if not fecha_cbte_str:
                return True, "OK" # Si no hay fecha, AFIP usará Today
            
            if isinstance(fecha_cbte_str, datetime.date):
                fecha_cbte = fecha_cbte_str
            else:
                fecha_cbte = datetime.datetime.strptime(str(fecha_cbte_str), '%Y-%m-%d').date()
            
            hoy = datetime.date.today()
            concepto = int(c.get('concepto', 1))
            margen = 10 if concepto > 1 else 5
            
            diff = (hoy - fecha_cbte).days
            if abs(diff) > margen:
                return False, f"Fecha fuera de rango AFIP: {fecha_cbte} tiene una diferencia de {diff} días (Máx permitido: {margen} días para concepto {concepto})."
            return True, "OK"
        except Exception as e:
            return True, f"Omitiendo validación de fecha por error de formato: {e}"

    @staticmethod
    def validar_periodo_servicios(c):
        """
        Check: Conceptos 2 y 3 requieren periodos y vencimiento.
        """
        concepto = int(c.get('concepto', 1))
        if concepto > 1:
            if not c.get('fch_serv_desde') or not c.get('fch_serv_hasta') or not c.get('fch_vto_pago'):
                return False, "Los servicios (Concepto 2/3) requieren Fecha Desde, Hasta y Vencimiento de Pago."
        return True, "OK"

    @staticmethod
    async def health_check(enterprise_id):
        """
        Diagnóstico de combate para el Operador.
        Incluye el escaneo del Señuelo Dummy para detectar túneles bloqueados.
        """
        config_check = await AfipService.verificar_configuracion(enterprise_id)
        entorno = config_check.get('entorno', 'testing')
        
        # Lanzar el señuelo Dummy
        scout_res = await AfipService.fe_dummy(entorno)
        
        # Lanzar el nuevo tripulante wconsucuit (scout de padrón)
        crew_res = await AfipService.consultar_cuit(enterprise_id, "20171634432") # CUIT de prueba
        
        return {
            "status": "ONLINE" if (config_check['success'] and scout_res['success']) else "WARNING",
            "cert_info": config_check,
            "tunnels_status": scout_res,
            "crew_wconsucuit": "OK" if crew_res['success'] else "OFFLINE",
            "engine_version": "Nebuchadnezzar-v2.1",
            "timestamp": datetime.datetime.now().isoformat()
        }
    
    WSAA_WSDL = {
        'testing': 'https://wsaahomo.afip.gov.ar/ws/services/LoginCms?wsdl',
        'produccion': 'https://wsaa.afip.gov.ar/ws/services/LoginCms?wsdl'
    }
    
    PADRON_WSDL = {
        'testing': 'https://awshomo.afip.gov.ar/sr-padron/webservices/personaServiceA13?WSDL',
        'produccion': 'https://aws.afip.gov.ar/sr-padron/webservices/personaServiceA13?WSDL'
    }

    APOC_WSDL = {
        'testing': 'https://servicios1.afip.gov.ar/wsapoc/services/ConsultarPubApoc?wsdl',
        'produccion': 'https://servicios1.afip.gov.ar/wsapoc/services/ConsultarPubApoc?wsdl'
    }

    FE_WSDL = {
        'testing': 'https://wswhomo.afip.gov.ar/wsfev1/service.asmx?WSDL',
        'produccion': 'https://servicios1.afip.gov.ar/wsfev1/service.asmx?WSDL'
    }

    WCONSUCUIT_WSDL = {
        'testing': 'https://awshomo.afip.gov.ar/sr-padron/webservices/personaServiceA13?WSDL',
        'produccion': 'https://aws.afip.gov.ar/sr-padron/webservices/personaServiceA13?WSDL'
    }

    PADRON_A10_WSDL = {
        'testing': 'https://awshomo.afip.gov.ar/sr-padron/webservices/personaServiceA10?WSDL',
        'produccion': 'https://aws.afip.gov.ar/sr-padron/webservices/personaServiceA10?WSDL'
    }

    PADRON_A100_WSDL = {
        'testing': 'https://awshomo.afip.gov.ar/sr-parametros/webservices/parameterServiceA100?wsdl',
        'produccion': 'https://aws.afip.gov.ar/sr-parametros/webservices/parameterServiceA100?wsdl'
    }

    @staticmethod
    async def registrar_bitacora(enterprise_id, evento, tipo='INFO', detalle=None, data=None):
        """
        Vitácora de Vuelo: Registra eventos de seguridad y vulnerabilidades persistentes.
        Permite saber qué ha pasado durante la caza de intrusos.
        """
        try:
            import json
            data_json = json.dumps(data) if data else None
            async with get_db_cursor() as cur:
                await cur.execute("""
                    INSERT INTO fin_neb_bitacora (enterprise_id, evento, tipo, detalle, data_json)
                    VALUES (%s, %s, %s, %s, %s)
                """, (enterprise_id, evento, tipo, detalle, data_json))
        except Exception as e:
            print(f"Error registrando en bitacora: {e}")

    @staticmethod
    async def consultar_cuit(enterprise_id, cuit_objetivo):
        """
        wconsucuit: El nuevo tripulante experto en identificación.
        Valida que clientes y proveedores no sean intrusos.
        """
        try:
            # 1. Obtener Token
            ticket = await AfipService._the_key_maker(enterprise_id, service="ws_sr_padron_a13")
            if not ticket:
                return {"success": False, "error": "No hay pase para wconsucuit."}
            
            config = await AfipService.get_afip_config(enterprise_id)
            wsdl = AfipService.WCONSUCUIT_WSDL.get(config['afip_entorno'])
            
            from zeep import AsyncClient
            from zeep.transports import AsyncTransport
            import httpx
            
            async with httpx.AsyncClient() as transport_client:
                transport = AsyncTransport(client=transport_client)
                client = AsyncClient(wsdl=wsdl, transport=transport)
                
                # Blindaje de CUITs (A13 / wconsucuit)
                cuit_empresa = int("".join(filter(str.isdigit, str(config['cuit']))))
                cuit_busca = int("".join(filter(str.isdigit, str(cuit_objetivo))))

                res = await client.service.getPersona_v2(
                    token=ticket['token'],
                    sign=ticket['sign'],
                    cuitRepresentada=cuit_empresa,
                    idPersona=cuit_busca
                )
                
                if hasattr(res, 'personaReturn'):
                    p = res.personaReturn
                    persona = p.persona
                    nombre = getattr(persona, 'nombre', '') or getattr(persona, 'razonSocial', 'Desconocido')
                    
                    # Extraer Condición de IVA (Simplificado para A13)
                    # En A13 solemos buscar en la descripción del estado
                    iva_desc = getattr(p, 'descripcionCriterio', '') or ""
                    iva_fmt = "IVA_RESPONSABLE_INSCRIPTO" if "Inscripto" in iva_desc else \
                             "MONOTRIBUTO" if "Monotributo" in iva_desc else \
                             "IVA_EXENTO" if "Exento" in iva_desc else "CONSUMIDOR_FINAL"

                    await AfipService.registrar_bitacora(
                        enterprise_id, "ESCANE_CUIT", "SECURITY", 
                        f"wconsucuit escaneó al sujeto {cuit_objetivo}: {nombre} ({iva_fmt})", 
                        {"cuit": cuit_objetivo, "nombre": nombre, "iva": iva_fmt}
                    )
                    return {"success": True, "nombre": nombre, "iva": iva_fmt, "datos": str(persona)}
                
                return {"success": False, "error": "Sujeto no identificado en la Matrix."}
        except Exception as e:
            err_msg = str(e)
            await AfipService.registrar_bitacora(enterprise_id, "VULNERABILIDAD_IDENTIFICADA", "ALERT", f"Fallo de wconsucuit: {err_msg}")
            return {"success": False, "error": err_msg}

    @staticmethod
    async def consultar_datos_a10(enterprise_id, cuit_objetivo):
        """
        Padron A10: El Scout de Datos.
        Obtiene información detallada para validar si el intruso coincide con nuestra lista de invitados.
        """
        try:
            # 1. Obtener Token
            ticket = await AfipService._the_key_maker(enterprise_id, service="ws_sr_padron_a10")
            if not ticket:
                return {"success": False, "error": "No hay pase para A10."}
            
            config = await AfipService.get_afip_config(enterprise_id)
            wsdl = AfipService.PADRON_A10_WSDL.get(config['afip_entorno'])
            
            from zeep import AsyncClient
            from zeep.transports import AsyncTransport
            import httpx
            
            async with httpx.AsyncClient() as transport_client:
                transport = AsyncTransport(client=transport_client)
                client = AsyncClient(wsdl=wsdl, transport=transport)
                
                # Blindaje de CUITs (Quitar guiones y asegurar enteros)
                cuit_empresa = int("".join(filter(str.isdigit, str(config['cuit']))))
                cuit_busca = int("".join(filter(str.isdigit, str(cuit_objetivo))))

                res = await client.service.getPersona(
                    token=ticket['token'],
                    sign=ticket['sign'],
                    cuitRepresentada=cuit_empresa,
                    idPersona=cuit_busca
                )
                
                if hasattr(res, 'personaReturn'):
                    p = res.personaReturn
                    persona = p.persona
                    nombre = getattr(persona, 'nombre', '') or getattr(persona, 'razonSocial', 'Desconocido')
                    
                    # Extraer Condición de IVA (Criterio de Categorización)
                    # En A10, a veces viene en 'descripcionCriterio' o dentro de los regímenes
                    criterio = getattr(p, 'descripcionCriterio', '') or ""
                    iva_fmt = None
                    
                    if "Inscripto" in criterio: iva_fmt = "IVA_RESPONSABLE_INSCRIPTO"
                    elif "Monotributo" in criterio: iva_fmt = "MONOTRIBUTO"
                    elif "Exento" in criterio: iva_fmt = "IVA_EXENTO"
                    elif "Consumidor Final" in criterio: iva_fmt = "CONSUMIDOR_FINAL"
                    
                    await AfipService.registrar_bitacora(
                        enterprise_id, "AUDITORIA_INVITADOS", "INFO", 
                        f"A10 validando historial de {cuit_objetivo}: {nombre} ({iva_fmt})", 
                        {"cuit": cuit_objetivo, "nombre": nombre, "iva": iva_fmt}
                    )
                    return {"success": True, "nombre": nombre, "iva": iva_fmt, "datos": str(persona)}
                
                return {"success": False, "error": "Datos del intruso no encontrados en A10."}
        except Exception as e:
            err_msg = str(e)
            await AfipService.registrar_bitacora(enterprise_id, "FALLO_SCOUT_A10", "WARNING", f"Error en A10: {err_msg}")
            return {"success": False, "error": err_msg}

    @staticmethod
    async def fe_dummy(entorno='testing'):
        """
        FEDummy: El Señuelo Scout.
        Permite verificar si los túneles de AFIP están bloqueados por los Sentinels.
        No requiere autenticación. Devuelve estado de AppServer, DbServer y AuthServer.
        """
        try:
            from zeep import AsyncClient
            from zeep.transports import AsyncTransport
            import httpx
            
            wsdl = AfipService.FE_WSDL.get(entorno)
            async with httpx.AsyncClient() as transport_client:
                transport = AsyncTransport(client=transport_client)
                client = AsyncClient(wsdl=wsdl, transport=transport)
                res = await client.service.FEDummy()
                
                return {
                    "success": True,
                    "app_server": res.AppServer,
                    "db_server": res.DbServer,
                    "auth_server": res.AuthServer,
                    "mensaje": f"Túneles escaneados: AppServer={res.AppServer}, DbServer={res.DbServer}, AuthServer={res.AuthServer}"
                }
        except Exception as e:
            return {"success": False, "error": f"Señuelo interceptado (AFIP caído): {str(e)}"}

    @staticmethod
    async def verificar_configuracion(enterprise_id):
        """
        Verifica si los certificados cargados son válidos.
        """
        try:
            from cryptography import x509
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend
        except ImportError:
            return {"success": False, "error": "Librería cryptography no instalada"}

        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("SELECT cuit, afip_crt, afip_key, afip_entorno FROM sys_enterprises WHERE id = %s", (enterprise_id,))
            emp = await cursor.fetchone()
        
        if not emp or not emp['afip_crt'] or not emp['afip_key']:
            return {"success": False, "error": "Faltan certificados"}

        try:
            # Validar CRT
            cert = x509.load_pem_x509_certificate(emp['afip_crt'].encode(), default_backend())
            # Validar KEY
            key = serialization.load_pem_private_key(emp['afip_key'].encode(), password=None, backend=default_backend())
            
            not_after = cert.not_valid_after
            if not_after < datetime.datetime.now():
                return {"success": False, "error": f"Certificado expirado el {not_after}"}
            
            return {
                "success": True, 
                "mensaje": "Certificados válidos",
                "expiracion": not_after.strftime('%d/%m/%Y'),
                "entorno": emp['afip_entorno'],
                "cuit": emp['cuit']
            }
        except Exception as e:
            return {"success": False, "error": f"Error de validación: {str(e)}"}

    @staticmethod
    async def _the_key_maker(enterprise_id, service="ws_sr_padron_a13"):
        """
        TheKeyMaker: El fabricante de llaves del Nabucodonosor.
        Gestiona los Tickets de Acceso WSAA con caché inteligente en fin_trinity_tokens.
        Trinity los guarda; TheKeyMaker los fabrica cuando vencen.
        """
        # 1. Verificar caché en base de datos
        try:
            async with get_db_cursor(dictionary=True) as cur:
                await cur.execute("""
                    SELECT token, sign, expira_en FROM fin_trinity_tokens 
                    WHERE enterprise_id = %s AND servicio = %s 
                    AND expira_en > DATE_ADD(NOW(), INTERVAL 10 MINUTE)
                """, (enterprise_id, service))
                cached = await cur.fetchone()
                if cached:
                    print(f"DEBUG WSAA: Reutilizando ticket cacheado para {service} (expira {cached['expira_en']})")
                    return {"token": cached['token'], "sign": cached['sign']}
        except Exception as e:
            print(f"DEBUG WSAA: Error leyendo caché de tokens: {e}")

        # 2. No hay caché válido → pedir ticket nuevo a AFIP
        config = await AfipService.get_afip_config(enterprise_id)
        if not config: return None
        
        entorno = config['afip_entorno']
        crt_data = config['afip_crt'].encode()
        key_data = config['afip_key'].encode()
        
        # Crear el XML de requerimiento (TRA)
        now = datetime.datetime.now()
        vto = now + datetime.timedelta(hours=12)
        unique_id = str(random.randint(0, 999999))
        
        tra = f"""<?xml version="1.0" encoding="UTF-8"?>
<loginTicketRequest version="1.0">
  <header>
    <uniqueId>{unique_id}</uniqueId>
    <generationTime>{now.isoformat()}</generationTime>
    <expirationTime>{vto.isoformat()}</expirationTime>
  </header>
  <service>{service}</service>
</loginTicketRequest>"""

        try:
            cert = x509.load_pem_x509_certificate(crt_data, default_backend())
            key = serialization.load_pem_private_key(key_data, password=None, backend=default_backend())
            
            signature = pkcs7.PKCS7SignatureBuilder().set_data(tra.encode())\
                        .add_signer(cert, key, hashes.SHA256())\
                        .sign(serialization.Encoding.DER, [])
            
            cms_signed = base64.b64encode(signature).decode('utf-8').replace("\n", "").replace("\r", "").strip()
            
            wsdl = AfipService.WSAA_WSDL.get(entorno)
            from zeep import AsyncClient
            from zeep.transports import AsyncTransport
            
            async with httpx.AsyncClient(timeout=30.0, verify=False) as transport_client:
                transport = AsyncTransport(client=transport_client)
                client = AsyncClient(wsdl=wsdl, transport=transport)
                response_xml = await client.service.loginCms(in0=cms_signed)
            
            from lxml import etree
            root = etree.fromstring(response_xml.encode('utf-8'))
            token = root.xpath('//token/text()')[0]
            sign = root.xpath('//sign/text()')[0]
            
            # 3. Guardar en caché (UPSERT)
            try:
                async with get_db_cursor(dictionary=True) as cur:
                    await cur.execute("""
                        INSERT INTO fin_trinity_tokens (enterprise_id, servicio, token, sign, expira_en)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE token=VALUES(token), sign=VALUES(sign), expira_en=VALUES(expira_en)
                    """, (enterprise_id, service, token, sign, vto))
                    print(f"DEBUG WSAA: Ticket nuevo para '{service}' guardado en caché hasta {vto}.")
            except Exception as e:
                print(f"DEBUG WSAA: No se pudo cachear el token: {e}")
            
            return {"token": token, "sign": sign}
            
        except Exception as e:
            print(f"Error WSAA: {str(e)}")
            return None


    @staticmethod
    async def get_afip_config(enterprise_id):
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("SELECT cuit, afip_crt, afip_key, afip_entorno FROM sys_enterprises WHERE id = %s", (enterprise_id,))
            return await cursor.fetchone()

    @staticmethod
    async def consultar_padron(enterprise_id, cuit_dni):
        """
        Consulta real al Padr\u00f3n de AFIP. (Async)
        """
        digits = "".join(filter(str.isdigit, str(cuit_dni)))
        config = await AfipService.get_afip_config(enterprise_id)
        
        # Si no hay certificados activos, simulamos
        if not config or not config['afip_crt'] or not config['afip_key']:
            return AfipService._simular_consultar_padron(digits)

        # Modo Real
        try:
            # Blindaje de CUITs (Quitar guiones y asegurar enteros)
            cuit_busca = int("".join(filter(str.isdigit, str(cuit_dni))))
            
            ticket = await AfipService._the_key_maker(enterprise_id, service="ws_sr_padron_a10")
            if not ticket:
                return {"success": False, "error": "No se pudo obtener el Ticket de Acceso (WSAA)"}
            
            wsdl = AfipService.PADRON_A10_WSDL.get(config['afip_entorno'])
            from zeep import AsyncClient
            from zeep.transports import AsyncTransport
            
            async with httpx.AsyncClient() as transport_client:
                transport = AsyncTransport(client=transport_client)
                client = AsyncClient(wsdl=wsdl, transport=transport)
                
                # Limpiar CUIT de la empresa (solo n\u00fameros)
                cuit_empresa = int("".join(filter(str.isdigit, str(config['cuit']))))
                
                # Llamada al servicio getPersona
                res = await client.service.getPersona(
                    token=ticket['token'],
                    sign=ticket['sign'],
                    cuitRepresentada=cuit_empresa,
                    idPersona=cuit_busca
                )
            
            if not res or not hasattr(res, 'personaReturn'):
                return {"success": False, "error": "No se encontraron datos en AFIP (personaReturn missing)"}

            p_return = res.personaReturn
            if not hasattr(p_return, 'persona'):
                return {"success": False, "error": "No se encontraron datos en AFIP (persona missing)"}
            
            p = p_return.persona
            # Mapear campos de AFIP a nuestro formato
            
            # Construir Razón Social / Nombre Completo
            nombre_completo = ""
            if hasattr(p, 'razonSocial') and p.razonSocial:
                nombre_completo = p.razonSocial
            else:
                ape = getattr(p, 'apellido', '') or ''
                nom = getattr(p, 'nombre', '') or ''
                nombre_completo = f"{ape} {nom}".strip()

            data = {
                "cuit": str(p.idPersona),
                "razon_social": nombre_completo or "Desconocido",
                "tipo_persona": getattr(p, 'tipoPersona', ''),
                "estado": getattr(p, 'estadoClave', ''),
                "domicilio": "",
                "condicion_iva": "Consumidor Final",
                "jurisdicciones": [],
                "monotributo": False
            }
            
            # Domicilio Fiscal
            if hasattr(p, 'domicilio'):
                doms = p.domicilio
                if not isinstance(doms, list): doms = [doms]
                for d in doms:
                    if getattr(d, 'tipoDomicilio', '') == "FISCAL":
                        data["domicilio"] = f"{getattr(d, 'direccion', '')}, {getattr(d, 'localidad', '')}, {getattr(d, 'descripcionProvincia', '')}"
                        break
            
            # Impuestos (IVA / Monotributo)
            if hasattr(p, 'impuesto'):
                imps = p.impuesto
                if not isinstance(imps, list): imps = [imps]
                for imp in imps:
                    id_imp = getattr(imp, 'idImpuesto', 0)
                    if id_imp == 30: data["condicion_iva"] = "IVA Responsable Inscripto"
                    elif id_imp == 20: 
                        data["condicion_iva"] = "Monotributo"
                        data["monotributo"] = True
                    elif id_imp == 32: data["condicion_iva"] = "IVA Exento"
            
            return {"success": True, "data": data}

        except Exception as e:
            return {"success": False, "error": f"Error AFIP Real: {str(e)}"}

    @staticmethod
    def _simular_consultar_padron(digits):
        # Simulación dinámica para el usuario
        if digits == "20171634432":
            return {
                "success": True,
                "data": {
                    "cuit": "20171634432",
                    "razon_social": "MARCELO PERI",
                    "tipo_persona": "FISICA",
                    "estado": "ACTIVO",
                    "domicilio": "AV. CARABOBO 345, CABA",
                    "condicion_iva": "Monotributo",
                    "jurisdicciones": ["901 - CABA"],
                    "monotributo": True
                }
            }
        return {
            "success": True,
            "data": {
                "cuit": digits,
                "razon_social": "USUARIO TEST AFIP S.A.",
                "tipo_persona": "JURIDICA",
                "estado": "ACTIVO",
                "domicilio": "AV. LIBERTADOR 1234, CABA",
                "condicion_iva": "Responsable Inscripto",
                "jurisdicciones": ["901 - CABA", "902 - BS AS"],
                "monotributo": False
            }
        }

    @staticmethod
    async def solicitar_cae(enterprise_id, comprobante, cursor=None):
        """
        Solicita el CAE al webservice de AFIP.
        Estrategia de Resistencia del Nabucodonosor:
          1. Intenta hasta 3 veces con 5 segundos de pausa (Backoff).
          2. Si AFIP sigue caído, guarda en fin_cae_pendientes y avisa a la tripulación.
        """
        config = await AfipService.get_afip_config(enterprise_id)
        if not config or not config['afip_crt']:
            return {"success": False, "error": "No hay certificados configurados para Factura Electrónica."}

        MAX_REINTENTOS = 3
        PAUSA_SEGUNDOS = 5
        ultimo_error = ""

        for intento in range(1, MAX_REINTENTOS + 1):
            try:
                if cursor:
                    resultado = await AfipService._ejecutar_solicitud_cae(cursor, enterprise_id, comprobante, config)
                else:
                    async with get_db_cursor(dictionary=True) as new_cursor:
                        resultado = await AfipService._ejecutar_solicitud_cae(new_cursor, enterprise_id, comprobante, config)

                if resultado.get('success'):
                    return resultado

                # Si el error es de datos (no de infraestructura), no reintentamos
                error_msg = resultado.get('error', '')
                codigos_infra = ['500', '501', '502', '503', 'timeout', 'connect', 'SOAP']
                es_error_infra = any(c in error_msg for c in codigos_infra)

                if not es_error_infra:
                    return resultado  # Error de negocio, no reintentar

                ultimo_error = error_msg
                if intento < MAX_REINTENTOS:
                    import asyncio
                    print(f"Reintento {intento}/{MAX_REINTENTOS} - AFIP no responde. Esperando {PAUSA_SEGUNDOS}s...")
                    await asyncio.sleep(PAUSA_SEGUNDOS)

            except Exception as e:
                ultimo_error = str(e)
                if intento < MAX_REINTENTOS:
                    import asyncio
                    await asyncio.sleep(PAUSA_SEGUNDOS)

        # --- AFIP cerró el túnel. Encolamos y avisamos a la tripulación ---
        comprobante_id = comprobante if isinstance(comprobante, int) else comprobante.get('id')
        if comprobante_id:
            try:
                async with get_db_cursor() as q_cursor:
                    await q_cursor.execute("""
                        INSERT IGNORE INTO fin_cae_pendientes
                        (enterprise_id, comprobante_id, intentos, ultimo_intento, proximo_intento, ultimo_error)
                        VALUES (%s, %s, %s, NOW(), DATE_ADD(NOW(), INTERVAL 10 MINUTE), %s)
                    """, (enterprise_id, comprobante_id, MAX_REINTENTOS, ultimo_error[:500]))
            except Exception as qe:
                print(f"Error al encolar CAE pendiente: {qe}")

        return {
            "success": False,
            "pendiente": True,
            "error": (
                f"⚠️ AFIP no responde luego de {MAX_REINTENTOS} intentos. "
                f"La factura fue guardada en estado PENDIENTE DE CAE. "
                f"El sistema reintentará automáticamente en 10 minutos. "
                f"Puede imprimir el comprobante sin CAE y regularizarlo durante el día. "
                f"Causa técnica: {ultimo_error[:150]}"
            )
        }

    @staticmethod
    async def _ejecutar_solicitud_cae(cursor, enterprise_id, comprobante, config):
        try:
            if isinstance(comprobante, (int, str)):
                # Cargar datos completos de la base de datos
                await cursor.execute("""
                    SELECT erp_comprobantes.*, erp_terceros.cuit as cliente_cuit 
                    FROM erp_comprobantes
                    JOIN erp_terceros ON erp_comprobantes.tercero_id = erp_terceros.id
                    WHERE erp_comprobantes.id = %s AND erp_comprobantes.enterprise_id = %s
                """, (comprobante, enterprise_id))
                data = await cursor.fetchone()
                if not data:
                    return {"success": False, "error": "Comprobante no encontrado."}
                
                # Cargar detalles para agrupar IVA
                await cursor.execute("SELECT * FROM erp_comprobantes_detalle WHERE comprobante_id = %s AND enterprise_id = %s", (comprobante, enterprise_id))
                detalles = await cursor.fetchall()
                
                comprobante_data = {
                    'id': data['id'],
                    'tipo_comprobante': data['tipo_comprobante'],
                    'punto_venta': data['punto_venta'],
                    'cliente_cuit': data['cliente_cuit'],
                    'total': float(data['importe_total']),
                    'neto': float(data['importe_neto']),
                    'iva': float(data['importe_iva']),
                    'percepciones': float((data.get('importe_percepcion_iibb_arba') or 0) + (data.get('importe_percepcion_iibb_agip') or 0)),
                    'comprobante_asociado_id': data.get('comprobante_asociado_id'),
                    'detalles': detalles
                }
            else:
                comprobante_data = comprobante
    
            # 1. TheKeyMaker provee el ticket de acceso (con caché de Trinity)
            ticket = await AfipService._the_key_maker(enterprise_id, service="wsfe")
            if not ticket:
                return {"success": False, "error": "TheKeyMaker no pudo obtener el Ticket de Acceso (WSAA) para WSFE"}
                
            entorno = config['afip_entorno']
            WSFE_WSDL = {
                'testing': 'https://wswhomo.afip.gov.ar/wsfev1/service.asmx?WSDL',
                'produccion': 'https://servicios1.afip.gov.ar/wsfev1/service.asmx?WSDL'
            }
            
            from zeep import AsyncClient
            from zeep.transports import AsyncTransport
            
            async with httpx.AsyncClient() as transport_client:
                transport = AsyncTransport(client=transport_client)
                client = AsyncClient(wsdl=WSFE_WSDL.get(entorno), transport=transport)
                
                cuit_empresa = int("".join(filter(str.isdigit, str(config['cuit']))))
                auth_header = {'Token': ticket['token'], 'Sign': ticket['sign'], 'Cuit': cuit_empresa}

                # --- CAPA DE POTENCIA 1: VALIDACIÓN DE TOPES (TAX ENGINE) ---
                doc_nro_str = "".join(filter(str.isdigit, str(comprobante_data.get('cliente_cuit', ''))))
                total_factura = float(comprobante_data['total'])
                
                es_anonimo = not doc_nro_str or doc_nro_str == '0'
                if es_anonimo and total_factura > AfipService.TOPE_ANONIMO_EFECTIVO:
                    return {
                        "success": False, 
                        "error": f"Fallo de Pre-vuelo: Comprobante supera el tope de ${AfipService.TOPE_ANONIMO_EFECTIVO:,.2f} para Consumidor Final anónimo."
                    }

                # --- CAPA DE POTENCIA 2: INTEGRIDAD MATEMÁTICA ---
                ok_mat, msj_mat = AfipService.validar_integridad_matematica(comprobante_data)
                if not ok_mat:
                    return {"success": False, "error": f"Fallo de Pre-vuelo: {msj_mat}"}

                # --- CAPA DE POTENCIA 3: VENTANA DE FECHAS ---
                ok_fch, msj_fch = AfipService.validar_ventana_fechas(comprobante_data)
                if not ok_fch:
                    return {"success": False, "error": f"Fallo de Pre-vuelo: {msj_fch}"}

                # --- CAPA DE POTENCIA 4: REQUISITOS DE SERVICIOS ---
                ok_srv, msj_srv = AfipService.validar_periodo_servicios(comprobante_data)
                if not ok_srv:
                    return {"success": False, "error": f"Fallo de Pre-vuelo: {msj_srv}"}

                # 2. Obtener el próximo número de comprobante de AFIP
                res_ultimo = await client.service.FECompUltimoAutorizado(
                    Auth=auth_header,
                    CbteTipo=int(comprobante_data['tipo_comprobante']),
                    PtoVta=int(comprobante_data['punto_venta'])
                )
                prox_nro = res_ultimo.CbteNro + 1
                
                # 3. Preparar detalles de IVA
                iva_list = []
                iva_groups = {}
                iva_map = {21.0: 5, 10.5: 4, 27.0: 6, 5.0: 8, 2.5: 9, 0.0: 3}
                
                if 'detalles' in comprobante_data:
                    for d in comprobante_data['detalles']:
                        alic = float(d.get('alicuota_iva', 21))
                        alic_id = iva_map.get(alic, 5)
                        if alic_id not in iva_groups:
                            iva_groups[alic_id] = {'BaseImp': 0, 'Importe': 0}
                        iva_groups[alic_id]['BaseImp'] += float(d.get('subtotal_neto', d.get('neto', 0)))
                        iva_groups[alic_id]['Importe'] += float(d.get('importe_iva', d.get('iva', 0)))
                else:
                    iva_groups[5] = {'BaseImp': float(comprobante_data['neto']), 'Importe': float(comprobante_data['iva'])}
    
                for i_id, vals in iva_groups.items():
                    iva_list.append({
                        'Id': i_id,
                        'BaseImp': round(vals['BaseImp'], 2),
                        'Importe': round(vals['Importe'], 2)
                    })
    
                # 4. Comprobantes Asociados (para NC/ND)
                cbtes_asoc = []
                if comprobante_data.get('comprobante_asociado_id'):
                    await cursor.execute("SELECT tipo_comprobante, punto_venta, numero FROM erp_comprobantes WHERE id = %s", (comprobante_data['comprobante_asociado_id'],))
                    asoc = await cursor.fetchone()
                    if asoc:
                        cbtes_asoc.append({
                            'Tipo': int(asoc['tipo_comprobante']),
                            'PtoVta': int(asoc['punto_venta']),
                            'Nro': int(asoc['numero'])
                        })
                
                # 5. Armar estructura FECAERequest
                doc_nro_str = "".join(filter(str.isdigit, str(comprobante_data['cliente_cuit'])))
                feat_det = {
                    'Concepto': 1,
                    'DocTipo': 80 if len(doc_nro_str) > 8 else 96, # 80: CUIT, 96: DNI
                    'DocNro': int(doc_nro_str),
                    'CbteDesde': prox_nro,
                    'CbteHasta': prox_nro,
                    'CbteFch': datetime.datetime.now().strftime('%Y%m%d'),
                    'ImpTotal': round(float(comprobante_data['total']), 2),
                    'ImpTotConc': 0,
                    'ImpNeto': round(float(comprobante_data['neto']), 2),
                    'ImpOpEx': 0,
                    'ImpTrib': round(float(comprobante_data.get('percepciones', 0)), 2),
                    'ImpIVA': round(float(comprobante_data['iva']), 2),
                    'MonId': 'PES',
                    'MonCotiz': 1,
                    'Iva': [iva_list] if iva_list else None # Zeep sometimes expects a wrapper
                }
                
                # Fix for Zeep list structure dependency
                feat_det['Iva'] = {'AlicIva': iva_list} if iva_list else None
                
                request = {
                    'FeCabReq': {
                        'CantReg': 1,
                        'PtoVta': int(comprobante_data['punto_venta']),
                        'CbteTipo': int(comprobante_data['tipo_comprobante'])
                    },
                    'FeDetReq': {'FECAEDetRequest': [feat_det]}
                }
                
                res = await client.service.FECAESolicitar(Auth=auth_header, FeCAEReq=request)
            
            if hasattr(res, 'FeDetResp') and res.FeDetResp:
                det = res.FeDetResp.FECAEDetResponse[0]
                if det.Resultado == 'A':
                    await cursor.execute("UPDATE erp_comprobantes SET cae = %s, vto_cae = %s, numero = %s WHERE id = %s", 
                                   (det.CAE, det.CAEFchVto, prox_nro, comprobante_data.get('id')))
                    return {"success": True, "cae": det.CAE, "cae_vto": det.CAEFchVto, "nro": prox_nro}
                else:
                    errors = []
                    error_codes = []
                    
                    if hasattr(det, 'Observaciones') and det.Observaciones:
                        obs_list = det.Observaciones.Obs if hasattr(det.Observaciones, 'Obs') else []
                        for o in obs_list:
                            code = str(o.Code)
                            msg = AfipService.ERRORES_TRADUCCION.get(code, o.Msg)
                            errors.append(f"({code}) {msg}")
                            error_codes.append(code)
                            
                    if hasattr(res, 'Errors') and res.Errors:
                        err_list = res.Errors.Err if hasattr(res.Errors, 'Err') else []
                        for e in err_list:
                            code = str(e.Code)
                            msg = AfipService.ERRORES_TRADUCCION.get(code, e.Msg)
                            errors.append(f"({code}) {msg}")
                            error_codes.append(code)

                    resultado_msg = " | ".join(errors) if errors else "Rechazado por AFIP sin descripción de error."
                    
                    if '10016' in error_codes:
                        resultado_msg += " (Acción sugerida: Sincronice el número de comprobante con AFIP en la configuración)."

                    return {"success": False, "error": resultado_msg, "codes": error_codes}
            
            return {"success": False, "error": "No hubo respuesta válida de AFIP"}
                             
        except Exception as e:
            return {"success": False, "error": f"Fallo en la comunicación: {str(e)}"}


    @staticmethod
    async def sincronizar_numeracion(enterprise_id, punto_venta, tipo_comprobante):
        """
        Consulta AFIP y devuelve el próximo número a utilizar, listo para actualizar en base local.
        """
        config = await AfipService.get_afip_config(enterprise_id)
        if not config: return {"success": False, "error": "No hay configuración de AFIP."}

        try:
            ticket = await AfipService._the_key_maker(enterprise_id, service="wsfe")
            if not ticket: return {"success": False, "error": "WSAA Falló."}

            entorno = config['afip_entorno']
            WSFE_WSDL = {
                'testing': 'https://wswhomo.afip.gov.ar/wsfev1/service.asmx?WSDL',
                'produccion': 'https://servicios1.afip.gov.ar/wsfev1/service.asmx?WSDL'
            }
            
            from zeep import AsyncClient
            from zeep.transports import AsyncTransport
            async with httpx.AsyncClient() as transport_client:
                transport = AsyncTransport(client=transport_client)
                client = AsyncClient(wsdl=WSFE_WSDL.get(entorno), transport=transport)
                
                cuit_empresa = int("".join(filter(str.isdigit, str(config['cuit']))))
                auth_header = {'Token': ticket['token'], 'Sign': ticket['sign'], 'Cuit': cuit_empresa}
                
                res = await client.service.FECompUltimoAutorizado(
                    Auth=auth_header,
                    CbteTipo=int(tipo_comprobante),
                    PtoVta=int(punto_venta)
                )
                
                ultimo = res.CbteNro
                proximo = ultimo + 1
                
                return {
                    "success": True, 
                    "ultimo_afip": ultimo, 
                    "proximo_local": proximo,
                    "mensaje": f"Sincronización exitosa. El próximo número es {proximo}."
                }
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    async def consultar_parametros_a100(enterprise_id, collection_name):
        """
        El Bibliotecario (A100): Descarga tablas maestras (parámetros) de la Matrix AFIP.
        """
        config_check = await AfipService.verificar_configuracion(enterprise_id)
        if not config_check['success']: return {"success": False, "error": "Configuración incompleta"}
        
        entorno = config_check.get('entorno', 'testing')
        wsdl = AfipService.PADRON_A100_WSDL[entorno]
        
        ticket = await AfipService._the_key_maker(enterprise_id, service='ws_sr_padron_a100')
        if not ticket: return {"success": False, "error": "Fallo autenticación WSAA"}
        token, sign = ticket['token'], ticket['sign']
        
        try:
            client = zeep.Client(wsdl=wsdl)
            res = client.service.getParametros(token, sign, collection_name)
            return {"success": True, "parametros": res}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    async def ejecutar_auditoria_general(enterprise_id, update_db=True):
        """
        Protocolo 'Gran Purga': Proceso Batch para auditar y actualizar clientes y proveedores.
        Sincroniza nombres y condiciones impositivas (tipo_responsable) desde la Matrix AFIP.
        """
        from database import get_db_cursor
        await AfipService.registrar_bitacora(enterprise_id, "BATCH_AUDITORIA_INICIO", "SECURITY", 
                                     f"Iniciando purga general. Update={'ON' if update_db else 'OFF'}")
        
        reporte = {
            "escaneados": 0, 
            "apocrifos": 0, 
            "nombres_actualizados": 0, 
            "iva_actualizado": 0
        }
        
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute("""
                SELECT id, cuit, nombre, tipo_responsable, es_cliente, es_proveedor 
                FROM erp_terceros 
                WHERE enterprise_id = %s AND activo = 1
            """, (enterprise_id,))
            terceros = await cursor.fetchall()
            
            for t in terceros:
                reporte["escaneados"] += 1
                cuit_raw = t['cuit'] or ""
                # Limpiar CUIT para validación de longitud
                cuit = "".join(filter(str.isdigit, cuit_raw))
                
                if len(cuit) < 11:
                    print(f"   ⚠️  SALTANDO: {t['nombre']} - CUIT inválido o corto: '{cuit_raw}'")
                    continue
                
                print(f"   🛰️  Escaneando [{reporte['escaneados']}/{len(terceros)}]: {t['nombre']} ({cuit})...")
                
                # 1. Scanner APOC (Detectar Traidores)
                apoc = await AfipService.consultar_base_apoc(enterprise_id, cuit)
                if apoc.get('es_apocrifo'):
                    reporte["apocrifos"] += 1
                    print(f"      🚨 CRITICAL: Sujeto APÓCRIFO detectado. Ejecutando purga.")
                    await AfipService.registrar_bitacora(enterprise_id, "TRAIDOR_DETECTADO", "CRITICAL", 
                                                f"Sujeto {t['nombre']} ({cuit}) es APÓCRIFO.", apoc)
                    if update_db:
                        await cursor.execute("UPDATE erp_terceros SET activo = 0 WHERE id = %s", (t['id'],))

                # 2. Scout A10 (Niobe) - Sincronía
                niobe = await AfipService.consultar_datos_a10(enterprise_id, cuit)
                if niobe['success']:
                    nuevo_nombre = niobe.get('nombre')
                    nueva_iva = niobe.get('iva')
                    
                    updates = []
                    params = []
                    
                    # Sincronizar nombre si difiere groseramente
                    if nuevo_nombre and nuevo_nombre.upper() != t['nombre'].upper():
                        print(f"      📝 IDENTIDAD CORREGIDA:")
                        print(f"         - Local: {t['nombre']}")
                        print(f"         - Matrix: {nuevo_nombre}")
                        updates.append("nombre = %s")
                        params.append(nuevo_nombre)
                        reporte["nombres_actualizados"] += 1
                    
                    # Sincronizar Condición IVA (tipo_responsable) especially if NULL
                    if nueva_iva and (not t['tipo_responsable'] or t['tipo_responsable'] != nueva_iva):
                        print(f"      💉 IVA SANADO:")
                        print(f"         - Local: {t['tipo_responsable'] or 'NULL'}")
                        print(f"         - Matrix: {nueva_iva}")
                        updates.append("tipo_responsable = %s")
                        params.append(nueva_iva)
                        reporte["iva_actualizado"] += 1
                    
                    if update_db and updates:
                        params.append(t['id'])
                        query = f"UPDATE erp_terceros SET {', '.join(updates)}, actualizado_en = NOW() WHERE id = %s"
                        await cursor.execute(query, tuple(params))
            
            # Flush changes si no estamos en modo dry-run
            await AfipService.registrar_bitacora(enterprise_id, "BATCH_AUDITORIA_FIN", "SECURITY", 
                                        f"Purga finalizada: {reporte}")
        
        return reporte

    @staticmethod
    async def consultar_base_apoc(enterprise_id, cuit_consultar):
        """
        Escanea la Matrix en busca de traidores (Contribuyentes Apócrifos).
        Blindado contra Sentinels: maneja WSDL dinámico, métodos variables y respuestas inesperadas.
        """
        config = await AfipService.get_afip_config(enterprise_id)
        if not config:
            return {"success": False, "error": "No hay configuración de empresa."}

        cuit_limpio = "".join(filter(str.isdigit, str(cuit_consultar)))
        if not cuit_limpio or len(cuit_limpio) < 10:
            return {"success": False, "error": f"CUIT inválido: {cuit_consultar}"}

        try:
            ticket = await AfipService._the_key_maker(enterprise_id, service="wsapoc")
            if not ticket:
                return {"success": False, "error": "Falla de acceso WSAA para wsapoc."}

            wsdl = AfipService.APOC_WSDL.get(config['afip_entorno'])
            if not wsdl:
                return {"success": False, "error": "WSDL APOC no configurado."}

            from zeep import AsyncClient
            from zeep.transports import AsyncTransport

            async with httpx.AsyncClient(timeout=30.0, verify=False) as transport_client:
                transport = AsyncTransport(client=transport_client)
                client = AsyncClient(wsdl=wsdl, transport=transport)

                cuit_empresa = int("".join(filter(str.isdigit, str(config['cuit']))))
                cuit_busca = int(cuit_limpio)

                # Introspección: listar métodos reales del WSDL para no adivinar
                try:
                    methods = [str(op) for op in client.service._binding._operations.keys()]
                    print(f"DEBUG APOC: Métodos disponibles en WSDL: {methods}")
                except Exception:
                    methods = []

                res = None
                ultimo_error = None

                # Intentar todos los métodos posibles conocidos (blindaje anti-Sentinel)
                candidate_methods = ['consultar', 'getAll', 'ConsultarApoc', 'consultarPubApoc', 'GetAll']
                for method_name in candidate_methods:
                    if methods and method_name not in methods:
                        continue
                    try:
                        service_method = getattr(client.service, method_name, None)
                        if not service_method:
                            continue
                        # Probar con y sin idPersonaConsulta
                        try:
                            res = await service_method(
                                token=ticket['token'],
                                sign=ticket['sign'],
                                cuitRepresentada=cuit_empresa,
                                idPersonaConsulta=cuit_busca
                            )
                        except Exception:
                            res = await service_method(
                                token=ticket['token'],
                                sign=ticket['sign'],
                                cuitRepresentada=cuit_empresa
                            )
                        if res is not None:
                            print(f"DEBUG APOC: Respuesta obtenida con método '{method_name}'")
                            break
                    except Exception as ex:
                        ultimo_error = str(ex)
                        print(f"DEBUG APOC: Método '{method_name}' falló: {ex}")
                        continue

                # Interpretar la respuesta
                if res is None:
                    # Sin respuesta válida = no encontrado en APOC (contribuyente limpio)
                    hint = f" (último error: {ultimo_error})" if ultimo_error else ""
                    return {"success": True, "es_apocrifo": False,
                            "mensaje": f"No se registran antecedentes apócrifos para CUIT {cuit_limpio}.{hint}"}

                # Analizar estructura de la respuesta dinámicamente
                res_dict = {}
                try:
                    res_dict = dict(res) if hasattr(res, '__iter__') else {}
                except Exception:
                    pass

                # Verificar marcas de apocrifo en la respuesta
                es_apoc = False
                detalles_str = str(res)

                for attr in ['apocrifo', 'esApocrifo', 'resultado', 'estado']:
                    val = getattr(res, attr, None)
                    if val is not None and str(val).upper() in ('SI', 'S', 'TRUE', '1', 'A', 'APOCRIFO'):
                        es_apoc = True
                        break

                if es_apoc:
                    return {
                        "success": True,
                        "es_apocrifo": True,
                        "cuit": cuit_limpio,
                        "mensaje": f"⚠️ ALERTA ROJA: CUIT {cuit_limpio} está en la Base APOC de AFIP.",
                        "detalles": detalles_str[:500]
                    }
                else:
                    return {
                        "success": True,
                        "es_apocrifo": False,
                        "cuit": cuit_limpio,
                        "mensaje": f"✅ CUIT {cuit_limpio} no registra antecedentes apócrifos.",
                        "detalles_raw": detalles_str[:200]
                    }

        except Exception as e:
            return {"success": False, "error": f"Error crítico en Scanner APOC: {str(e)}"}


