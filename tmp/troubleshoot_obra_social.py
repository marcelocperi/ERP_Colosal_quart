
import asyncio
import os
import sys

# Detect root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from database import get_db_cursor
from services.afip_service import AfipService

async def troubleshoot_obra_social():
    enterprise_id = 0
    cuit_to_find = '33628185889'
    
    print(f"🕵️ INFORME DE INTELIGENCIA: BUSCANDO A 'LA OBRA SOCIAL' ({cuit_to_find})")
    print("=" * 80)
    
    # 1. Búsqueda profunda en la base de datos local
    print("🔎 1. Escaneo en erp_terceros (por CUIT o Nombre)...")
    with get_db_cursor(dictionary=True) as cursor:
        # Buscar por CUIT exacto, con guiones o sin guiones
        cursor.execute("""
            SELECT id, cuit, nombre, tipo_responsable, activo 
            FROM erp_terceros 
            WHERE cuit LIKE %s OR nombre LIKE %s
        """, ('%' + cuit_to_find.replace('-', '') + '%', '%OBRA SOCIAL%PETROLEO%'))
        terceros = cursor.fetchall()
        
        if terceros:
            for t in terceros:
                print(f"   📦 ENCONTRADO EN DB: ID={t['id']} | CUIT='{t['cuit']}' | NOMBRE='{t['nombre']}' | IVA='{t['tipo_responsable']}' | ACTIVO={t['activo']}")
        else:
            print("   ❌ No se encontró nada similar en la base de datos local.")

    # 2. Consulta al Oráculo A13 (wconsucuit) - Suele ser más robusto para estos casos
    print("\n📡 2. Consultando al Oráculo A13 (wconsucuit) en la Matrix...")
    niobe_a13 = await AfipService.consultar_cuit(enterprise_id, cuit_to_find)
    
    if niobe_a13['success']:
        print(f"   ✅ A13 RESPONDIÓ:")
        print(f"      - Razón Social: {niobe_a13.get('nombre')}")
        print(f"      - Condición IVA: {niobe_a13.get('iva')}")
        
        # 3. Intentar sanación si lo encontramos arriba
        if terceros:
            print("\n💉 3. Ejecutando sanación forzada sobre los registros encontrados...")
            for t in terceros:
                with get_db_cursor() as cursor:
                    # Actualizar con lo que dice AFIP
                    cursor.execute("""
                        UPDATE erp_terceros 
                        SET nombre = %s, tipo_responsable = %s, actualizado_en = NOW()
                        WHERE id = %s
                    """, (niobe_a13['nombre'], niobe_a13['iva'], t['id']))
            print("   ✨ DB Actualizada exitosamente.")
    else:
        print(f"   ❌ A13 NO PUDO IDENTIFICARLO: {niobe_a13.get('error')}")

if __name__ == "__main__":
    asyncio.run(troubleshoot_obra_social())
