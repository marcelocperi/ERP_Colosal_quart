
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from services.local_intelligence_service import LocalIntelligenceService

print("Verificando servidor Ollama...")
is_up = LocalIntelligenceService.check_health()
print(f"Estado de Ollama: {'ACTIVO' if is_up else 'INACTIVO'}")

if is_up:
    print("\nConsultando Reglas de Auditoría al LLM Local...")
    res = LocalIntelligenceService.consult_rules("¿Cuáles son los 4 campos obligatorios para trazabilidad en Colosal ERP?")
    if "response" in res:
        print("\nRESPUESTA DEL LLM:")
        print("-" * 50)
        print(res["response"])
        print("-" * 50)
    else:
        print(f"Error: {res.get('error')}")
else:
    print("Por favor, inicie la aplicación Ollama en su escritorio.")
