
import asyncio
import os
import sys

# Detectar la raíz del proyecto (un nivel arriba de /tmp)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from services.afip_service import AfipService

async def purge_batch():
    # --- CONFIGURACIÓN DE LA MISIÓN ---
    enterprise_id = 0  # Nave principal
    update_db = True   # SET TO FALSE PARA DRY-RUN (Simulación)
    # --------------------------------
    
    print("\n" + "="*80)
    print("🚢 NABUCODONOSOR TAX ENGINE — PROTOCOLO: 'LA GRAN PURGA' (BATCH MODE)")
    print("="*80)
    
    # 1. PRE-FLIGHT CHECK (Túneles y Parámetros)
    print("📡 Pasito 1: Verificando túneles con el Señuelo FEDummy...")
    dummy = await AfipService.fe_dummy()
    if not dummy['success']:
        print(f"❌ ERROR: Túneles bloqueados por Sentinels. Abortando misión: {dummy['error']}")
        return
    print(f"✅ Túneles ONLINE: App={dummy['app_server']} Db={dummy['db_server']}")

    print("\n📚 Pasito 2: Consultando al Bibliotecario (A100) para sincronía de parámetros...")
    config_check = AfipService.verificar_configuracion(enterprise_id)
    entorno = config_check.get('entorno', 'testing')
    
    # Ejemplo: Validar que el Bibliotecario responda
    bibliotecario = await AfipService.consultar_parametros_a100(enterprise_id, "tipoResponsable")
    if bibliotecario['success']:
        print(f"✅ Bibliotecario A100 en línea. Datos de la Matrix sincronizados ({entorno}).")
    else:
        print(f"⚠️  ADVERTENCIA: El Bibliotecario está ocupado. Usando caché local de parámetros.")

    print("\n🎯 Pasito 3: Iniciando Purga Masiva de Sujetos (erp_terceros)...")
    print(f"🛡️  MODO DE OPERACIÓN: {'ACTIVO (Escritura en DB)' if update_db else 'SIMULACIÓN (Sólo Lectura)'}")
    print("-" * 80)
    
    try:
        # Ejecutar la auditoría profunda
        reporte = await AfipService.ejecutar_auditoria_general(enterprise_id, update_db=update_db)
        
        print("\n" + "="*80)
        print("📊 REPORTE FINAL DE LA MISIÓN: 'LA GRAN PURGA'")
        print("="*80)
        print(f"  🔍 Sujetos Escaneados:    {reporte['escaneados']}")
        print(f"  💀 Traidores (APOC) Purgados: {reporte['apocrifos']}")
        print(f"  📝 Nombres Sincronizados:   {reporte['nombres_actualizados']}")
        print(f"  💉 Condiciones IVA Sanadas:  {reporte['iva_actualizado']}")
        print("-" * 80)
        
        if reporte['nombres_actualizados'] > 0:
            print(f"💡 El sujeto 'IMAGEN TEST S A' (CUIT 30452157779) y otros han sido recalibrados")
            print(f"   según el registro oficial de la Matrix {entorno.upper()}.")
            
        print(f"\n✅ Misión completada. Los detalles residen en la Vitácora de Vuelo.")
        if not update_db:
            print(f"⚠️  ADVERTENCIA: El sistema operó en modo DRY-RUN. La base de datos no fue alterada.")
            
    except Exception as e:
        print(f"❌ ERROR CRÍTICO EN EL NÚCLEO: {str(e)}")
    
    print("="*80 + "\n")

if __name__ == "__main__":
    asyncio.run(purge_batch())
