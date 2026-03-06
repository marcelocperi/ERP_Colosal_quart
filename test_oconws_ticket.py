import asyncio
from services.afip_service import AfipService

async def test_ticket_oconws():
    print("Intentando obtener Ticket para 'oconws'...")
    ticket = await AfipService._obtener_login_ticket(0, service="oconws")
    if ticket:
        print("✅ ÉXITO: WSAA reconoce el servicio 'oconws'")
        print(f"Token: {ticket['token'][:20]}...")
    else:
        print("❌ FALLO: WSAA no devolvió ticket para 'oconws'")

if __name__ == "__main__":
    asyncio.run(test_ticket_oconws())
