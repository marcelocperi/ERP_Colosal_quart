
import random
import re
from database import get_db_cursor

def populate_full_padrones():
    try:
        with get_db_cursor() as cursor:
            # 1. Get all jurisdictions
            cursor.execute("SELECT nombre, codigo_jurisdiccion FROM sys_provincias")
            provincias = cursor.fetchall()
            
            # Map for shorthand names if needed, otherwise use the database names
            juris_list = [p[0] for p in provincias]
            
            # 2. Get all CUITs from erp_terceros
            cursor.execute("SELECT DISTINCT cuit FROM erp_terceros WHERE cuit IS NOT NULL AND cuit != ''")
            cuits_raw = [r[0] for r in cursor.fetchall()]
            
            # 3. Clear existing padron entries to start fresh and clean
            print("Limpiando padrón anterior...")
            cursor.execute("DELETE FROM sys_padrones_iibb")
            
            padron_data = []
            
            print(f"Poblando {len(cuits_raw)} CUITs en {len(juris_list)} jurisdicciones...")
            
            for cuit in cuits_raw:
                cuit_clean = re.sub(r'\D', '', str(cuit))
                if not cuit_clean: continue
                
                for juris in juris_list:
                    # Logic: 
                    # Most have some alicuota (1.5% - 3.5%)
                    # Some are exempt (0%)
                    # Some have high risk (4%+)
                    
                    dice = random.random()
                    if dice < 0.15: # 15% Exempt
                        alic_perc = 0.0
                        alic_ret = 0.0
                        riesgo = 0
                    elif dice > 0.90: # 10% High risk
                        alic_perc = round(random.uniform(0.04, 0.06), 4)
                        alic_ret = round(random.uniform(0.04, 0.06), 4)
                        riesgo = 3
                    else: # Standard range
                        alic_perc = round(random.uniform(0.01, 0.035), 4)
                        alic_ret = round(random.uniform(0.01, 0.035), 4)
                        riesgo = 1
                    
                    padron_data.append((juris, cuit_clean, alic_perc, alic_ret, riesgo))

            # 4. Batch insert
            if padron_data:
                # Break into chunks to avoid too large query
                chunk_size = 1000
                for i in range(0, len(padron_data), chunk_size):
                    chunk = padron_data[i:i + chunk_size]
                    cursor.executemany("""
                        INSERT INTO sys_padrones_iibb (jurisdiccion, cuit, alicuota_percepcion, alicuota_retencion, grupo_riesgo)
                        VALUES (%s, %s, %s, %s, %s)
                    """, chunk)
            
            print(f"\n✅ Sincronización Completa: {len(padron_data)} registros insertados.")
            print("Ahora todos los proveedores/clientes tienen alícuotas configuradas en todas las provincias.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    populate_full_padrones()
