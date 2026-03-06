
import random
import requests
import time
from database import get_db_cursor

CP_MAP = {
    "Ciudad Autónoma de Buenos Aires": 1000,
    "Buenos Aires": 1900,
    "Catamarca": 4700,
    "Chaco": 3500,
    "Chubut": 9100,
    "Córdoba": 5000,
    "Corrientes": 3400,
    "Entre Ríos": 3100,
    "Formosa": 3600,
    "Jujuy": 4600,
    "La Pampa": 6300,
    "La Rioja": 5300,
    "Mendoza": 5500,
    "Misiones": 3300,
    "Neuquén": 8300,
    "Río Negro": 8500,
    "Salta": 4400,
    "San Juan": 5400,
    "San Luis": 5700,
    "Santa Cruz": 9400,
    "Santa Fe": 3000,
    "Santiago del Estero": 4200,
    "Tierra del Fuego, Antártida e Islas del Atlántico Sur": 9410,
    "Tucumán": 4000
}

API_CALLES = "https://apis.datos.gob.ar/georef/api/v2.0/calles"

def get_real_streets(prov_nombre):
    try:
        params = {"provincia": prov_nombre, "max": 100}
        resp = requests.get(API_CALLES, params=params, timeout=10)
        data = resp.json()
        return data.get("calles", [])
    except Exception as e:
        print(f"Error fetching streets for {prov_nombre}: {e}")
        return []

def complete_addresses():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Get all addresses with "Calle Ficticia" or empty calle
            cursor.execute("SELECT id, provincia, etiqueta FROM erp_direcciones WHERE calle = 'Calle Ficticia' OR calle IS NULL OR calle = ''")
            direcciones = cursor.fetchall()
            print(f"Procesando {len(direcciones)} direcciones...")

            # cache streets per province to avoid excessive API calls
            prov_cache = {}

            for addr in direcciones:
                prov = addr['provincia']
                if prov not in prov_cache:
                    print(f"Cargando calles reales para {prov}...")
                    prov_cache[prov] = get_real_streets(prov)
                    time.sleep(0.2) # be nice to API

                streets = prov_cache.get(prov, [])
                if not streets:
                    continue

                # Pick a random street
                street_data = random.choice(streets)
                calle_nombre = street_data['nombre']
                
                # Height logic
                altura_info = street_data.get('altura', {})
                inicio = 1
                fin = 3000
                
                if altura_info:
                    # Inicia logic
                    i_der = altura_info.get('inicio', {}).get('derecha')
                    i_izq = altura_info.get('inicio', {}).get('izquierda')
                    if i_der is not None and i_izq is not None:
                        inicio = min(i_der, i_izq)
                    elif i_der is not None: inicio = i_der
                    elif i_izq is not None: inicio = i_izq

                    # Fin logic
                    f_der = altura_info.get('fin', {}).get('derecha')
                    f_izq = altura_info.get('fin', {}).get('izquierda')
                    if f_der is not None and f_izq is not None:
                        fin = max(f_der, f_izq)
                    elif f_der is not None: fin = f_der
                    elif f_izq is not None: fin = f_izq

                # Clamp to 1-3000 as per instruction
                inicio = max(1, inicio)
                fin = min(3000, fin)
                if fin < inicio: fin = inicio + 100
                
                numero = random.randint(inicio, fin)
                
                # Infer CP
                cp_base = CP_MAP.get(prov, 1000)
                # add some variation based on numerical range or just use base
                # Actually, CP is very specific to the city, but here we "infer"
                # Variation: if prov has many CPs, maybe add a random offset
                cp = cp_base + random.randint(0, 50) if prov != "Ciudad Autónoma de Buenos Aires" else cp_base + random.randint(1, 400)

                # Update
                cursor.execute("""
                    UPDATE erp_direcciones 
                    SET calle = %s, numero = %s, cod_postal = %s
                    WHERE id = %s
                """, (calle_nombre, str(numero), str(cp), addr['id']))

            print("\nActualización de direcciones completada.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    complete_addresses()
