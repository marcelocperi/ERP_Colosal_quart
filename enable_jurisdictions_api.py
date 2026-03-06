
import random
from database import get_db_cursor

JURISDICCION_CODES = {
    "Ciudad Autónoma de Buenos Aires": 901,
    "Buenos Aires": 902,
    "Catamarca": 903,
    "Córdoba": 904,
    "Corrientes": 905,
    "Chaco": 906,
    "Chubut": 907,
    "Entre Ríos": 908,
    "Formosa": 909,
    "Jujuy": 910,
    "La Pampa": 911,
    "La Rioja": 912,
    "Mendoza": 913,
    "Misiones": 914,
    "Neuquén": 915,
    "Río Negro": 916,
    "Salta": 917,
    "San Juan": 918,
    "San Luis": 919,
    "Santa Cruz": 920,
    "Santa Fe": 921,
    "Santiago del Estero": 922,
    "Tierra del Fuego, Antártida e Islas del Atlántico Sur": 923,
    "Tucumán": 924
}

def enable_jurisdictions_api():
    try:
        with get_db_cursor() as cursor:
            # 1. Add column to sys_provincias if not exists
            print("--- Actualizando sys_provincias ---")
            cursor.execute("SHOW COLUMNS FROM sys_provincias LIKE 'codigo_jurisdiccion'")
            if not cursor.fetchone():
                print("Agregando columna codigo_jurisdiccion...")
                cursor.execute("ALTER TABLE sys_provincias ADD COLUMN codigo_jurisdiccion INT AFTER nombre")
            
            # 2. Update codes
            for prov_name, code in JURISDICCION_CODES.items():
                cursor.execute("UPDATE sys_provincias SET codigo_jurisdiccion = %s WHERE nombre = %s", (code, prov_name))
            
            # 3. Populate Padrón with sample data
            print("\n--- Generando Padrón IIBB Simulado ---")
            # Get all unique CUITs in the system
            cursor.execute("SELECT DISTINCT cuit FROM erp_terceros")
            cuits = [r[0] for r in cursor.fetchall()]
            
            # Clear previous simulated data to avoid duplicates if re-run
            # Warning: this deletes all padron data. In this context it's okay for simulation.
            cursor.execute("DELETE FROM sys_padrones_iibb")
            
            padron_data = []
            jurisdicciones = ['ARBA', 'AGIP'] # Main ones for simulation
            
            import re
            for cuit in cuits:
                cuit_clean = re.sub(r'\D', '', str(cuit))
                if not cuit_clean: continue
                for juris in jurisdicciones:
                    # Random alicuotas between 0 and 4%
                    alic_perc = round(random.uniform(0, 0.04), 4)
                    alic_ret = round(random.uniform(0, 0.04), 4)
                    # For some, set 0 (exemptions)
                    if random.random() < 0.2:
                        alic_perc = 0.0
                        alic_ret = 0.0
                    
                    padron_data.append((juris, cuit_clean, alic_perc, alic_ret, 1)) # grupo_riesgo 1
            
            if padron_data:
                cursor.executemany("""
                    INSERT INTO sys_padrones_iibb (jurisdiccion, cuit, alicuota_percepcion, alicuota_retencion, grupo_riesgo)
                    VALUES (%s, %s, %s, %s, %s)
                """, padron_data)
                print(f"Insertados {len(padron_data)} registros de padrón para {len(cuits)} CUITs.")

            print("\n✅ API de Jurisdicciones habilitada y poblada.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    enable_jurisdictions_api()
