
import asyncio
import os
import sys

# Detect root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from database import get_db_cursor
from services.afip_service import AfipService

async def check():
    cuit = '20223689028'
    print(f"--- CHECKING LOCAL DB FOR {cuit} ---")
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, cuit, nombre, tipo_responsable FROM erp_terceros WHERE cuit LIKE %s", ('%'+cuit+'%',))
        rows = cursor.fetchall()
        for r in rows:
            print(f"ID={r['id']} | CUIT='{r['cuit']}' | NAME='{r['nombre']}' | RESP='{r['tipo_responsable']}'")
    
    print(f"\n--- CONSULTING AFIP (Padron A10) FOR {cuit} ---")
    res = await AfipService.consultar_padron(0, cuit)
    print(f"Result: {res}")

if __name__ == "__main__":
    asyncio.run(check())
