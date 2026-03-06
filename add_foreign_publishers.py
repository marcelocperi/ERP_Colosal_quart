
import sys
import os
# Adjust path to import database module
sys.path.append(os.path.join(os.path.dirname(__file__)))
from database import get_db_cursor
import traceback

# 1. Definir CUITs genéricos por país (50 + Código País + Serial)
COUNTRY_CUITS = {
    'US': '50-00000001-0', # Estados Unidos
    'ES': '50-00000034-0', # España
    'UK': '50-00000044-0', # Reino Unido
    'DE': '50-00000049-0', # Alemania
    'FR': '50-00000033-0', # Francia
    'IT': '50-00000039-0', # Italia
    'NL': '50-00000031-0', # Países Bajos
    'BR': '50-00000055-0', # Brasil
    'MX': '50-00000052-0', # México
    'PT': '50-00000351-0', # Portugal
    'CH': '50-00000041-0', # Suiza
    'BE': '50-00000032-0', # Bélgica
}

# 2. Mapeo manual de Editoriales a Países
PUBLISHER_COUNTRIES = {
    # US / UK / English
    "Independently Published": "US",
    "CreateSpace Independent Publishing Platform": "US",
    "Creative Media Partners, LLC": "US",
    "Blurb": "US",
    "Lulu Press, Inc.": "US",
    "Oxford University Press": "UK",
    "Cambridge University Press": "UK",
    "Routledge": "UK",
    "Palgrave Macmillan": "UK",
    "HarperCollins": "US",
    "HarperCollins Publishers": "US",
    "Wiley": "US",
    "John Wiley & Sons": "US",
    "McGraw-Hill": "US",
    "McGraw-Hill Education": "US",
    "Scholastic": "US",
    "Random House": "US",
    "Penguin Books": "UK",
    "Bloomsbury Publishing": "UK",
    "Bloomsbury Publishing Plc": "UK",
    "Taylor & Francis": "UK",
    "Babelcube Inc": "US",
    "Scribner": "US",
    "Prentice Hall": "US",
    "Ballantine Books": "US",
    "Harvard University Press": "US",
    "Little, Brown": "US",
    "Simon & Schuster": "US",
    "Houghton Mifflin Harcourt": "US",
    "Penguin Random House": "US",
    "Benchmark Education Company": "US",
    "Rowman & Littlefield": "US",
    "Rowman & Littlefield Publishers, Incorporated": "US",
    "International Monetary Fund": "US",
    "World Bank": "US",
    "Independent Publisher": "US",
    "OECD Publishing": "FR",
    "UNESCO": "FR",
    "Stationery Office": "UK",
    "The Stationery Office": "UK",
    
    # España
    "Alfaguara": "ES",
    "Debate": "ES",
    "Planeta": "ES",
    "Taurus": "ES",
    "Plaza & Janés": "ES",
    "Grijalbo": "ES",
    "Reservoir Books": "ES",
    "Salamandra": "ES",
    "Anagrama": "ES",
    "Lumen": "ES",
    "Tusquets": "ES",
    "Seix Barral": "ES",
    "Crítica": "ES",
    "Paidós": "ES",
    "Ediciones B": "ES",
    "Destino": "ES",
    "Alianza": "ES",
    "Akal": "ES",
    "Trotta": "ES",
    "Cátedra": "ES",
    "Tecnos": "ES",
    "Galaxia Gutenberg": "ES",
    "Siruela": "ES",
    "Visor": "ES",
    "Turner": "ES",
    "Pre-Textos": "ES",
    "Anagrama, Editorial": "ES",
    "Debolsillo": "ES",
    "Roca Editorial": "ES",
    "Alba Editorial": "ES",
    "Acantilado": "ES",
    "Ediciones Salamandra": "ES",
    "Malpaso": "ES",
    "Libros del Asteroide": "ES",
    "Impedimenta": "ES",
    "Periférica": "ES",
    "Sexto Piso": "ES",
    
    # Alemania
    "GRIN Verlag GmbH": "DE",
    "Springer": "DE",
    "Springer Berlin Heidelberg": "DE",
    "De Gruyter": "DE",
    "Walter de Gruyter": "DE",
    "Kohlhammer": "DE",
    "Kohlhammer, W., GmbH": "DE",
    "Suhrkamp": "DE",
    "C.H. Beck": "DE",
    "Peter Lang": "DE", 
    "Lit": "DE",
    "Lit Verlag": "DE",
    "Books on Demand GmbH": "DE",
    "De Gruyter, Inc.": "DE",
    
    # Italia
    "Giuffrè": "IT",
    "Marsilio": "IT",
    "Einaudi": "IT",
    "Mondadori": "IT",
    "Laterza": "IT",
    "Il Mulino": "IT",
    "Carocci": "IT",
    "Newton Compton": "IT",
    "Rizzoli": "IT",
    "Bompiani": "IT",
    "Adelphi": "IT",
    "Garzanti": "IT",
    "Feltrinelli": "IT",
    "Giunti": "IT",
    "San Paolo": "IT",
    "EGEA": "IT",
    "Zanichelli": "IT",
    "Cedam": "IT",
    "L'Erma di Bretschneider": "IT",
    "Aras": "IT",
    "Bulzoni": "IT",
    "FrancoAngeli": "IT",
    "F. Angeli": "IT",
    "Electa": "IT",
    "Skira": "IT",
    "Skira Editore": "IT",
    "Mimesis": "IT",
    "Gangemi": "IT",
    "Polistampa": "IT",
    "L'ornitorinco": "IT",
    "Edizioni dell'Orso": "IT",
    "Utet": "IT",
    "Cedam": "IT",
    "Giappichelli": "IT",
    "G. Giappichelli": "IT",
    "Centro editoriale toscano": "IT",
    
    # Francia
    "Gallimard": "FR",
    "Seuil": "FR",
    "L'Harmattan": "FR",
    "Harmattan": "FR",
    "PUF": "FR",
    "Presses Universitaires de France": "FR",
    "Flammarion": "FR",
    "Albin Michel": "FR",
    "Larousse": "FR",
    "Hachette": "FR",
    "Fayard": "FR",

    # Países Bajos
    "Kluwer": "NL",
    "Wolters Kluwer": "NL",
    "Brill": "NL",
    "Elsevier": "NL",
    "Staatsuitgeverij": "NL",
    "Walburg Pers": "NL",
    "Querido": "NL",
    "Amsterdam University Press": "NL",
    "Contact": "NL",
    "De Arbeiderspers": "NL",
    "Arbeiderspers": "NL",
    "Boom": "NL",
    "Ten Have": "NL",
    "Sijthoff": "NL",
    "Kok": "NL",
    "Prometheus": "NL",
    "De Bataafsche Leeuw": "NL",
    "Wolters-Noordhoff": "NL",
    "De Bezige Bij": "NL",
    "Atlas": "NL",
    "Samsom": "NL",
    "Verloren": "NL",
    "Waanders": "NL",
    "Het Spectrum": "NL",
    "Aksant": "NL",
    "In de Knipscheer": "NL",
    
    # Brasil
    "Companhia das Letras": "BR",
    "Editora Saraiva": "BR",
    "Editora Vozes": "BR",
    "Annablume": "BR",
    "Ática": "BR",
    "Moderna": "BR",
    "Bertrand Brasil": "BR",
    "Summus": "BR",
    "Imprensa Oficial": "BR",
    "EDIPUCRS": "BR",
    "EdUEM": "BR",
    
    # México
    "Fondo de Cultura Económica": "MX",
    "Siglo XXI": "MX",
    "Era": "MX",
    "Porrúa": "MX",
    
    # Portugal
    "Leya": "PT",
    "Bertrand": "PT",
    "Bertrand Editora": "PT",
    "Publicações Dom Quixote": "PT",
    "Editorial Presença": "PT",
    "Assírio & Alvim": "PT",
    "Porto Editora": "PT",
    
    # Suiza / Otros
    "Stämpfli": "CH",
    "Peter Lang AG": "CH",
    "P. Lang": "CH",
    "Peter Lang": "CH",
    "Lang, Peter": "CH",
    
    # Bélgica
    "Lannoo": "BE",
    "Acco": "BE",
    "Maklu": "BE",
    "Brepols": "BE",
    "Brepols Publishers": "BE",
    "Peeters": "BE",
    "Clavis": "BE",
    "Standaard": "BE",
    "Lannoo": "BE",
    "Acco": "BE",
    "Houtekiet": "BE"
}

def process_foreign_publishers():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute("SELECT id FROM sys_enterprises WHERE estado = 'activo'")
            enterprises = [r['id'] for r in cursor.fetchall()]
            
            print(f"Procesando {len(PUBLISHER_COUNTRIES)} editoriales extranjeras identificadas...")
            
            inserted_count = 0
            
            for pub_name, country_code in PUBLISHER_COUNTRIES.items():
                cuit = COUNTRY_CUITS.get(country_code)
                if not cuit: continue
                
                # print(f"-> {pub_name} ({country_code})")
                
                for ent_id in enterprises:
                    # Check existence
                    cursor.execute("SELECT id FROM erp_terceros WHERE enterprise_id = %s AND nombre = %s", 
                                   (ent_id, pub_name))
                    existing = cursor.fetchone()
                    
                    if existing:
                        continue
                        
                    # Create
                    cursor.execute("""
                        INSERT INTO erp_terceros 
                        (enterprise_id, nombre, cuit, es_proveedor, activo, tipo_responsable, observaciones)
                        VALUES (%s, %s, %s, 1, 1, 'Importación', %s)
                    """, (ent_id, pub_name, cuit, f"Editorial Extranjera ({country_code})"))
                    
                    tercero_id = cursor.lastrowid
                    inserted_count += 1
                    
                    # Create Dummy Address
                    cursor.execute("""
                        INSERT INTO erp_direcciones (enterprise_id, tercero_id, etiqueta, calle, numero, localidad, provincia, pais, es_fiscal, es_entrega)
                        VALUES (%s, %s, 'Sede', 'Extranjero', '0', 'Extranjero', 'Extranjero', %s, 1, 0)
                    """, (ent_id, tercero_id, country_code))
                    
                    # Create Fiscal Data
                    cursor.execute("""
                        INSERT INTO erp_datos_fiscales (enterprise_id, tercero_id, impuesto, jurisdiccion, condicion, numero_inscripcion, alicuota)
                        VALUES (%s, %s, 'IIBB', '999', 'No Alcanzado', %s, 0.00)
                    """, (ent_id, tercero_id, cuit))
            
            print(f"Proceso completado. Se insertaron {inserted_count} nuevos registros.")
            
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    process_foreign_publishers()
