
import asyncio
import os
import sys

# Detect root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from database import get_db_cursor
from services.afip_service import AfipService

async def main():
    enterprise_id = 0
    print("\n" + "="*80)
    print("🚀 SINCRONIZADOR DE PADRÓN AFIP (A13) — MODO AUDITORÍA")
    print("="*80)
    
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, cuit, nombre, tipo_responsable FROM erp_terceros WHERE enterprise_id = %s", (enterprise_id,))
        terceros = cursor.fetchall()
        
        print(f"📦 Total de registros a verificar: {len(terceros)}")
        print("-" * 80)
        
        counts = {"checks": 0, "updates": 0, "errors": 0}
        
        for t in terceros:
            counts["checks"] += 1
            cuit_local = t['cuit']
            if not cuit_local: continue
            
            # Limpiar CUIT para la consulta
            cuit_limpio = "".join(filter(str.isdigit, str(cuit_local)))
            if len(cuit_limpio) < 11: continue
            
            print(f"🔍 Escaneando [{counts['checks']}/{len(terceros)}]: {t['nombre']} ({cuit_local})...")
            
            try:
                # Consulta directa al Padrón
                res = await AfipService.consultar_padron(enterprise_id, cuit_limpio)
                
                if res['success']:
                    afip_data = res['data']
                    nombre_afip = afip_data['razon_social'].strip().upper()
                    
                    iva_desc = afip_data['condicion_iva']
                    iva_local = None
                    if "Inscripto" in iva_desc: iva_local = "IVA_RESPONSABLE_INSCRIPTO"
                    elif "Monotributo" in iva_desc: iva_local = "MONOTRIBUTO"
                    elif "Exento" in iva_desc: iva_local = "IVA_EXENTO"
                    else: iva_local = "CONSUMIDOR_FINAL"
                    
                    differences = []
                    updates = {}
                    
                    if nombre_afip and nombre_afip != t['nombre'].strip().upper():
                        differences.append(f"NOMBRE: '{t['nombre']}' -> '{nombre_afip}'")
                        updates['nombre'] = nombre_afip
                        
                    if iva_local and iva_local != t['tipo_responsable']:
                        differences.append(f"IVA: '{t['tipo_responsable']}' -> '{iva_local}'")
                        updates['tipo_responsable'] = iva_local
                        
                    if differences:
                        counts["updates"] += 1
                        print(f"   ✨ DISCREPANCIAS ENCONTRADAS:")
                        for d in differences:
                            print(f"      - {d}")
                        
                        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
                        values = list(updates.values()) + [t['id']]
                        cursor.execute(f"UPDATE erp_terceros SET {set_clause}, actualizado_en = NOW() WHERE id = %s", tuple(values))
                        print(f"   ✅ Base de datos actualizada.")
                else:
                    print(f"   ⚠️ AFIP REPORTA: {res.get('error')}")
                    # Registrar en bitácora para auditoría
                    AfipService.registrar_bitacora(enterprise_id, "SYNC_ERROR", "WARNING", 
                                                f"CUIT {cuit_local} no sincronizado: {res.get('error')}")
                    
            except Exception as e:
                counts["errors"] += 1
                print(f"   ❌ ERROR en CUIT {cuit_local}: {str(e)}")
        
        print("\n" + "="*80)
        print("📊 RESUMEN FINAL:")
        print(f"   - Procesados:   {counts['checks']}")
        print(f"   - Actualizados: {counts['updates']}")
        print(f"   - Errores:      {counts['errors']}")
        print("="*80 + "\n")

if __name__ == "__main__":
    asyncio.run(main())
