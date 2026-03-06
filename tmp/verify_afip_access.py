
import asyncio
import os
import sys

# Agregar el path del proyecto
project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from services.afip_service import AfipService

async def main():
    enterprise_id = 0
    print(f"--- Iniciando Escaneo de Túneles AFIP para Empresa ID {enterprise_id} ---")
    
    # 1. Comprobar Salud de Certificados
    health = AfipService.health_check(enterprise_id)
    print(f"\n[1] Estado de los Certificados:")
    if health['cert_info']['success']:
        print(f"✅ Certificados Válidos. Expiran el: {health['cert_info']['expiracion']}")
    else:
        print(f"❌ Error en Certificados: {health['cert_info']['error']}")
        return

    # 2. Probar Túnel WSFE (Factura Electrónica)
    print(f"\n[2] Probando Túnel WSFE (Factura Electrónica)...")
    ticket_wsfe = await AfipService._obtener_login_ticket(enterprise_id, service="wsfe")
    if ticket_wsfe:
        print(f"✅ Túnel WSFE Abierto. Token obtenido correctamente.")
        
        # Probar una consulta de numeración básica
        # Usamos PV 1 y Tipo 1 (Factura A) como prueba rápida
        sync = await AfipService.sincronizar_numeracion(enterprise_id, 1, 1)
        if sync['success']:
            print(f"✅ Sincronización WSFE Exitosa: Último Nro AFIP = {sync['ultimo_afip']}")
        else:
            print(f"⚠️ Alerta en WSFE: {sync['error']} (Esto es normal si el PV 1 no está habilitado en este CUIT)")
    else:
        print(f"❌ Túnel WSFE Bloqueado por AFIP.")

    # 3. Probar Túnel WSAPOC (Traidores)
    print(f"\n[3] Probando Túnel WSAPOC (Scanner de Traidores)...")
    ticket_apoc = await AfipService._obtener_login_ticket(enterprise_id, service="wsapoc")
    if ticket_apoc:
        print(f"✅ Túnel WSAPOC Abierto. Scanner de Traidores en línea.")
        
        # Consultar un CUIT genérico de prueba (ej: AFIP mismo o similar)
        apoc_check = await AfipService.consultar_base_apoc(enterprise_id, "20171634432")
        if apoc_check['success']:
            print(f"✅ Scanner APOC Funcionando: {apoc_check['mensaje']}")
        else:
            print(f"⚠️ Error en Scanner APOC: {apoc_check['error']}")
    else:
        print(f"❌ Túnel WSAPOC Bloqueado. Posiblemente falta autorizar el servicio en AFIP.")

    print(f"\n--- Escaneo de Túneles Finalizado ---")

if __name__ == "__main__":
    asyncio.run(main())
