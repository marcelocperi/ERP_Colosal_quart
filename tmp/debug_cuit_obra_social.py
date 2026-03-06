
import asyncio
import os
import sys

# Detect root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from database import get_db_cursor
from services.afip_service import AfipService

async def debug_cuit():
    enterprise_id = 0
    cuit_target = '33628185889'
    
    print(f"🔍 DEBUGGING CUIT: {cuit_target}")
    print("-" * 60)
    
    # 1. Check DB
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, cuit, nombre, tipo_responsable FROM erp_terceros WHERE cuit LIKE %s", ('%'+cuit_target+'%',))
        res = cursor.fetchall()
        if res:
            for r in res:
                print(f"📦 DB STATE: ID={r['id']} | CUIT={r['cuit']} | NAME='{r['nombre']}' | IVA='{r['tipo_responsable']}'")
        else:
            print("❌ CUIT NOT FOUND IN DATABASE")

    print("\n📡 CONSULTING AFIP (A10) - THE MATRIX...")
    # 2. Check AFIP A10
    niobe = await AfipService.consultar_datos_a10(enterprise_id, cuit_target)
    
    if niobe['success']:
        print(f"✅ AFIP RESPONSE:")
        print(f"   - Name:  {niobe.get('nombre')}")
        print(f"   - IVA:   {niobe.get('iva')}")
        print(f"   - Raw:   {niobe.get('datos')[:500]}...")
    else:
        print(f"❌ AFIP ERROR: {niobe.get('error')}")

if __name__ == "__main__":
    asyncio.run(debug_cuit())
