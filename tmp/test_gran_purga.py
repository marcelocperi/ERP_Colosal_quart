
import asyncio
import os
import sys

# Detectar la raíz del proyecto (un nivel arriba de /tmp)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from services.afip_service import AfipService
from database import get_db_cursor

async def test_purge():
    print("🚀 INICIANDO PROTOCOLO 'LA GRAN PURGA'...")
    enterprise_id = 0 # Usamos la ID 0 de la base de datos principal
    
    reporte = await AfipService.ejecutar_auditoria_general(enterprise_id)
    
    print("\n📊 REPORTE DE CAMPAÑA FINAL:")
    print(f"  - Terceros Escaneados: {reporte['proveedores_escaneados']}")
    print(f"  - Traidores (APOC) Detectados: {reporte['apocrifos_detectados']}")
    print(f"  - Discrepancias de Identidad: {reporte['discrepancias_id']}")
    print("\n✅ Los resultados han sido inyectados en la Vitácora de Vuelo.")

if __name__ == "__main__":
    asyncio.run(test_purge())
