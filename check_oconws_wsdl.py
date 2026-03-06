import httpx
import asyncio
from zeep import AsyncClient
from zeep.transports import AsyncTransport

async def check_wsdl():
    # Posibles URLs de homologación
    urls = [
        "https://fwshomo.afip.gov.ar/oconws/services/CONService?wsdl",
        "https://awshomo.afip.gov.ar/oconws/services/CONService?wsdl"
    ]
    
    for url in urls:
        print(f"Probando WSDL: {url}")
        try:
            async with httpx.AsyncClient(timeout=10.0, verify=False) as transport_client:
                transport = AsyncTransport(client=transport_client)
                client = AsyncClient(wsdl=url, transport=transport)
                print(f"✅ ÉXITO conectando a {url}")
                print("Servicios disponibles:")
                for service in client.wsdl.services.values():
                    print(f" - {service.name}")
                    for port in service.ports.values():
                        operations = port.binding._operations.values()
                        for op in operations:
                            print(f"   * Operation: {op.name}")
                return url
        except Exception as e:
            print(f"❌ Falló {url}: {str(e)}")
    return None

if __name__ == "__main__":
    asyncio.run(check_wsdl())
