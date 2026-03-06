
from database import get_db_cursor
import logging

logging.basicConfig(level=logging.INFO)

# Mapeo de Jurisdicciones IIBB (Convenio Multilateral)
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

def populate_details():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            # 1. Obtener todos los terceros que son proveedores (o clientes si se quiere repetir, pero aquí foco en proveedor)
            cursor.execute("SELECT id, enterprise_id, nombre, cuit FROM erp_terceros WHERE es_proveedor = 1")
            proveedores = cursor.fetchall()
            print(f"Procesando {len(proveedores)} proveedores...")

            # 2. Obtener provincias registradas
            cursor.execute("SELECT nombre FROM sys_provincias")
            provincias = [r['nombre'] for r in cursor.fetchall()]

            for prov in proveedores:
                print(f"  -> Proveedor: {prov['nombre']} (ID: {prov['id']})")
                
                for prov_nombre in provincias:
                    # A. Crear Dirección (Sede)
                    cursor.execute("""
                        SELECT id FROM erp_direcciones 
                        WHERE tercero_id = %s AND provincia = %s AND etiqueta = %s
                    """, (prov['id'], prov_nombre, f"Sede {prov_nombre}"))
                    
                    if not cursor.fetchone():
                        cursor.execute("""
                            INSERT INTO erp_direcciones (enterprise_id, tercero_id, etiqueta, calle, numero, localidad, provincia, cod_postal, es_fiscal, es_entrega)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            prov['enterprise_id'], 
                            prov['id'], 
                            f"Sede {prov_nombre}", 
                            "Calle Ficticia", "123", 
                            "Localidad Central", prov_nombre, "0000",
                            0, 1
                        ))

                    # B. Crear Dato Fiscal IIBB
                    juris_code = IIBB_CODES.get(prov_nombre, "000")
                    cursor.execute("""
                        SELECT id FROM erp_datos_fiscales 
                        WHERE tercero_id = %s AND impuesto = 'IIBB' AND jurisdiccion = %s
                    """, (prov['id'], juris_code))
                    
                    if not cursor.fetchone():
                        cursor.execute("""
                            INSERT INTO erp_datos_fiscales (enterprise_id, tercero_id, impuesto, jurisdiccion, condicion, numero_inscripcion, alicuota)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            prov['enterprise_id'],
                            prov['id'],
                            'IIBB',
                            juris_code,
                            'Inscripto',
                            prov['cuit'] or '00-00000000-0',
                            0.00
                        ))
            
            print("\nProceso finalizado correctamente.")

    except Exception as e:
        print(f"Error durante el proceso: {e}")

if __name__ == "__main__":
    populate_details()
