
import random
from database import get_db_cursor

IIBB_CODES = {
    "Ciudad Autónoma de Buenos Aires": "901",
    "Buenos Aires": "902",
    "Catamarca": "903",
    "Córdoba": "904",
    "Corrientes": "905",
    "Chaco": "906",
    "Chubut": "907",
    "Entre Ríos": "908",
    "Formosa": "909",
    "Jujuy": "910",
    "La Pampa": "911",
    "La Rioja": "912",
    "Mendoza": "913",
    "Misiones": "914",
    "Neuquén": "915",
    "Río Negro": "916",
    "Salta": "917",
    "San Juan": "918",
    "San Luis": "919",
    "Santa Cruz": "920",
    "Santa Fe": "921",
    "Santiago del Estero": "922",
    "Tucumán": "923",
    "Tierra del Fuego, Antártida e Islas del Atlántico Sur": "924"
}

PROVIDER_NAMES_RESERVE = [
    "Servicios Integrales", "ArgenTech Solutions", "Logística Federal", "Insumos Pro", "Suministros del Norte",
    "Argentina Global S.A.", "Patagonia Express", "Andes Provisiones", "Cuyo Distribución", "Litoral Insumos",
    "Norte Papel", "Sur Construcciones", "Centro Tecnológico", "Pampa Suministros", "Atlántico Seguridad",
    "Mega Distribuidora", "Plus Cargo Argentino", "Industrial B.A.", "Comercializadora Federal", "Insumos Médicos S.A.",
    "Todo Oficinas", "Provee Ar", "Master Proveedores", "Líder Logística", "Punto Norte S.R.L."
]

def generate_cuit():
    type_code = random.choice(["20", "23", "27", "30"])
    body = "".join([str(random.randint(0, 9)) for _ in range(8)])
    verify = str(random.randint(0, 9))
    return f"{type_code}-{body}-{verify}"

def ensure_jurisdictions():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Get provinces
            cursor.execute("SELECT id, nombre FROM sys_provincias")
            provincias = cursor.fetchall() # List of dicts {id, nombre}
            prov_names = [p['nombre'] for p in provincias]
            
            # 2. Get active enterprises
            cursor.execute("SELECT id FROM sys_enterprises WHERE estado = 'activo'")
            enterprises = [r['id'] for r in cursor.fetchall()]
            
            for ent_id in enterprises:
                print(f"Refinando jurisdicciones para empresa ID: {ent_id}")
                
                # A. Ensure we have at least 24 providers
                cursor.execute("SELECT id, nombre, cuit FROM erp_terceros WHERE es_proveedor = 1 AND enterprise_id = %s", (ent_id,))
                providers = cursor.fetchall()
                
                while len(providers) < 24:
                    name = random.choice(PROVIDER_NAMES_RESERVE) + " " + str(random.randint(100, 999))
                    cuit = generate_cuit()
                    cursor.execute("""
                        INSERT INTO erp_terceros (enterprise_id, nombre, cuit, es_proveedor, activo, tipo_responsable)
                        VALUES (%s, %s, %s, 1, 1, 'Resp. Inscripto')
                    """, (ent_id, name, cuit))
                    new_id = cursor.lastrowid
                    providers.append({'id': new_id, 'nombre': name, 'cuit': cuit})

                # B. Map each of the first 24 providers to a unique province
                random.shuffle(prov_names)
                for i in range(24):
                    prov_target = prov_names[i]
                    prov_id = next(p['id'] for p in provincias if p['nombre'] == prov_target)
                    prov_obj = providers[i]
                    
                    print(f"  Asignando {prov_target} a {prov_obj['nombre']}")
                    
                    # C. Get a real locality for this province
                    cursor.execute("SELECT nombre FROM sys_localidades WHERE provincia_id = %s ORDER BY RAND() LIMIT 1", (prov_id,))
                    loc_row = cursor.fetchone()
                    localidad = loc_row['nombre'] if loc_row else "Capital"

                    # D. Update or Insert Fiscal Address for this assigned province
                    # First, reset all other addresses of this provider as non-fiscal
                    cursor.execute("UPDATE erp_direcciones SET es_fiscal = 0 WHERE tercero_id = %s AND enterprise_id = %s", (prov_obj['id'], ent_id))
                    
                    # Update/Insert the target province address as FISCAL
                    cursor.execute("""
                        SELECT id FROM erp_direcciones 
                        WHERE tercero_id = %s AND provincia = %s AND enterprise_id = %s LIMIT 1
                    """, (prov_obj['id'], prov_target, ent_id))
                    
                    addr_id = cursor.fetchone()
                    if addr_id:
                        cursor.execute("""
                            UPDATE erp_direcciones 
                            SET es_fiscal = 1, localidad = %s, etiqueta = %s
                            WHERE id = %s
                        """, (localidad, f"Sede Fiscal {prov_target}", addr_id['id']))
                    else:
                        cursor.execute("""
                            INSERT INTO erp_direcciones (enterprise_id, tercero_id, etiqueta, calle, numero, localidad, provincia, cod_postal, es_fiscal, es_entrega)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, 1)
                        """, (ent_id, prov_obj['id'], f"Sede Fiscal {prov_target}", "Calle de la Jurisdicción", "10", localidad, prov_target, "1000"))

                    # E. Ensure all 24 IIBB records exist for this provider (901-924)
                    for p_name in prov_names:
                        code = IIBB_CODES.get(p_name, "000")
                        cursor.execute("""
                            SELECT id FROM erp_datos_fiscales 
                            WHERE tercero_id = %s AND impuesto = 'IIBB' AND jurisdiccion = %s AND enterprise_id = %s
                        """, (prov_obj['id'], code, ent_id))
                        
                        if not cursor.fetchone():
                            cursor.execute("""
                                INSERT INTO erp_datos_fiscales (enterprise_id, tercero_id, impuesto, jurisdiccion, condicion, numero_inscripcion, alicuota)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (ent_id, prov_obj['id'], 'IIBB', code, 'Inscripto', prov_obj['cuit'] or '0', 0.00))

            print("\nProceso de perfeccionamiento de jurisdicciones finalizado.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    ensure_jurisdictions()
