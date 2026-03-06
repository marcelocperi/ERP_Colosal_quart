
import asyncio
import os
import sys

# Detect root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from database import get_db_cursor
from services.afip_service import AfipService

async def main():
    cuit_target = '30452157779'
    print(f"--- DATABASE CHECK: {cuit_target} ---")
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, cuit, nombre, tipo_responsable FROM erp_terceros WHERE cuit LIKE %s", ('%'+cuit_target+'%',))
        res = cursor.fetchall()
        for r in res:
            print(f"ID: {r['id']} | CUIT: {r['cuit']} | NAME: {r['nombre']} | RESP: {r['tipo_responsable']}")
    
    print(f"\n--- AFIP SOURCE CHECK (A10) ---")
    niobe = await AfipService.consultar_datos_a10(0, cuit_target)
    print(f"A10 Result: {niobe}")

if __name__ == "__main__":
    asyncio.run(main())
