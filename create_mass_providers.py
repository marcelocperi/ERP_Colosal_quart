
import random
from database import get_db_cursor

PROVIDER_NAMES = [
    "Distribuidora del Norte", "Papelera Central", "Logística S.A.", "Muebles de Oficina Express",
    "TecnoSolutions Argentina", "Suministros Industriales S.A.", "Limpieza y Brillo",
    "Seguridad Total S.R.L.", "Catering Eventos", "Transportes Unidos", "Papelería Mundial",
    "Insumos Médicos del Sur", "Ferretería El Tornillo", "Uniformes y Bordados",
    "Soluciones Gráficas S.A.", "Editorial del Plata", "Librería San Martín", "Distribuidora de Bebidas",
    "Garantía de Calidad S.A.", "Maquinarias del Nordeste", "Construcciones Avanzadas",
    "Textil Buenos Aires", "Comunicaciones Plus", "Vivero Los Pinos", "Panificadora Industrial"
]

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

def generate_cuit():
    type_code = random.choice(["20", "23", "27", "30"])
    body = "".join([str(random.randint(0, 9)) for _ in range(8)])
    verify = str(random.randint(0, 9))
    return f"{type_code}-{body}-{verify}"

def create_providers():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # Get enterprises
            cursor.execute("SELECT id FROM sys_enterprises WHERE estado = 'activo'")
            enterprises = [r['id'] for r in cursor.fetchall()]
            
            # Get provinces
            cursor.execute("SELECT nombre FROM sys_provincias")
            provincias = [r['nombre'] for r in cursor.fetchall()]
            
            for ent_id in enterprises:
                print(f"Creando proveedores para empresa ID: {ent_id}")
                
                # Check current provider count for this enterprise
                cursor.execute("SELECT COUNT(*) as cnt FROM erp_terceros WHERE es_proveedor = 1 AND enterprise_id = %s", (ent_id,))
                current_count = cursor.fetchone()['cnt']
                
                needed = 15 - current_count
                if needed <= 0:
                    print(f"  La empresa {ent_id} ya tiene {current_count} proveedores.")
                    continue
                
                print(f"  Creando {needed} nuevos proveedores...")
                
                # Randomly pick names from PROVIDER_NAMES
                sample_names = random.sample(PROVIDER_NAMES, min(needed, len(PROVIDER_NAMES)))
                
                for name in sample_names:
                    cuit = generate_cuit()
                    # Insert Provider
                    cursor.execute("""
                        INSERT INTO erp_terceros (enterprise_id, nombre, cuit, es_proveedor, activo, tipo_responsable)
                        VALUES (%s, %s, %s, 1, 1, 'Resp. Inscripto')
                    """, (ent_id, f"{name} {random.randint(10, 99)}", cuit))
                    
                    prov_id = cursor.lastrowid
                    
                    # Assign Jurisdictions and Sedes (Logic from previous script)
                    for prov_nombre in provincias:
                        # Direction
                        cursor.execute("""
                            INSERT INTO erp_direcciones (enterprise_id, tercero_id, etiqueta, calle, numero, localidad, provincia, cod_postal, es_fiscal, es_entrega)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (ent_id, prov_id, f"Sede {prov_nombre}", "Av. Principal", str(random.randint(1, 9999)), "Localidad Demo", prov_nombre, "1000", 0, 1))

                        # Fiscal IIBB
                        juris_code = IIBB_CODES.get(prov_nombre, "000")
                        cursor.execute("""
                            INSERT INTO erp_datos_fiscales (enterprise_id, tercero_id, impuesto, jurisdiccion, condicion, numero_inscripcion, alicuota)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (ent_id, prov_id, 'IIBB', juris_code, 'Inscripto', cuit, 0.00))
                
                print(f"  Finalizada creación para empresa {ent_id}.")
            
        print("Proceso completo.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_providers()
