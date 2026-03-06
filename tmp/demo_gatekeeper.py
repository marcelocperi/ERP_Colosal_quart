
from services.local_intelligence_service import LocalIntelligenceService

def test_violation():
    print("--- SIMULACIÓN DE GATEKEEPER ---")
    
    # Escenario de propuesta de cambio que viola las normas
    cambio = "Quiero crear una tabla llamada 'stock_ajustes' para ajustes manuales. Tendrá campos id, producto_id y cantidad. No usaremos campos de usuario ni fecha para que sea más rápida la inserción."
    
    print(f"Propuesta del Usuario: {cambio}\n")
    print("Consultando al Auditor Colosal (Ollama Phi-3)...")
    
    resultado = LocalIntelligenceService.gatekeeper_check(cambio)
    
    if "response" in resultado:
        print("\n--- RESPUESTA DEL AUDITOR ---")
        print(resultado["response"])
    else:
        print(f"\nError: {resultado.get('error')}")

if __name__ == "__main__":
    test_violation()
