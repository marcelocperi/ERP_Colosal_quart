
import sys
import os
import sqlite3
# Adjust path to import database module
sys.path.append(os.path.join(os.path.dirname(__file__)))
from database import get_db_cursor

PROVIDERS = [
    {
        "nombre": "Penguin Random House Grupo Editorial S.A.",
        "cuit": "33-52701975-9",
        "direccion": "Humberto Primo",
        "numero": "555",
        "localidad": "Ciudad Autónoma de Buenos Aires",
        "provincia": "Ciudad Autónoma de Buenos Aires",
        "cp": "C1103ACK",
        "email": "recepcion.argentina@penguinrandomhouse.com",
        "web": "penguinrandomhousegrupoeditorial.com"
    },
    {
        "nombre": "Grupo Editorial Planeta S.A.I.C.",
        "cuit": "30-62537821-0",
        "direccion": "Ing. Enrique Butty",
        "numero": "275",  # Piso 8
        "localidad": "Ciudad Autónoma de Buenos Aires",
        "provincia": "Ciudad Autónoma de Buenos Aires",
        "cp": "C1001",
        "email": "infoar@planeta.com.ar",
        "web": "planetadelibros.com.ar"
    },
    {
        "nombre": "Riverside Agency S.A.C.",
        "cuit": "30-51629968-8",
        "direccion": "Av. Córdoba",
        "numero": "744", # Piso 5
        "localidad": "Ciudad Autónoma de Buenos Aires",
        "provincia": "Ciudad Autónoma de Buenos Aires",
        "cp": "C1054AAT",
        "email": "info@riveragency.com.ar",  # Hypothetical
        "web": "riverside-agency.com.ar"
    }
]

def add_providers():
    with get_db_cursor(dictionary=True) as cursor:
        # Get active enterprises
        cursor.execute("SELECT id FROM sys_enterprises WHERE estado = 'activo'")
        enterprises = [row['id'] for row in cursor.fetchall()]
        
        print(f"Agregando proveedores principales a {len(enterprises)} empresas...")
        
        for ent_id in enterprises:
            print(f"--> Procesando Empresa ID: {ent_id}")
            
            for provider in PROVIDERS:
                # Check if exists by CUIT
                cursor.execute("SELECT id FROM erp_terceros WHERE enterprise_id = %s AND cuit = %s", (ent_id, provider['cuit']))
                existing = cursor.fetchone()
                
                if existing:
                    print(f"    - {provider['nombre']} ya existe (ID: {existing['id']})")
                    continue
                
                # Insert Tercero
                print(f"    + Creando proveedor: {provider['nombre']}")
                cursor.execute("""
                    INSERT INTO erp_terceros (enterprise_id, nombre, cuit, es_proveedor, activo, tipo_responsable, email)
                    VALUES (%s, %s, %s, 1, 1, 'Inscripto', %s)
                """, (ent_id, provider['nombre'], provider['cuit'], provider['email']))
                
                tercero_id = cursor.lastrowid
                
                # Insert Address
                cursor.execute("""
                    INSERT INTO erp_direcciones (enterprise_id, tercero_id, etiqueta, calle, numero, localidad, provincia, cod_postal, es_fiscal, es_entrega)
                    VALUES (%s, %s, 'Sede Central', %s, %s, %s, %s, %s, 1, 1)
                """, (ent_id, tercero_id, provider['direccion'], provider['numero'], provider['localidad'], provider['provincia'], provider['cp']))
                
                # Insert Fiscal Data (Basic placeholder for IIBB CABA)
                cursor.execute("""
                    INSERT INTO erp_datos_fiscales (enterprise_id, tercero_id, impuesto, jurisdiccion, condicion, numero_inscripcion, alicuota)
                    VALUES (%s, %s, 'IIBB', '901', 'Inscripto', %s, 0.00)
                """, (ent_id, tercero_id, provider['cuit']))

        print("Proceso finalizado.")

if __name__ == "__main__":
    add_providers()
