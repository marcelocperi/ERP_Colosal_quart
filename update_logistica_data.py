from database import get_db_cursor
import requests

# Direcciones encontradas e investigadas para normalizar con Georef
# Formato: Nombre en BD -> Dirección para Georef
DIRECCIONES_RAW = {
    "Andreani Grupo Logístico": "Pienovi 104, Avellaneda, Buenos Aires",
    "DHL Supply Chain (Argentina) S.A.": "Av. Callao 1423, Ciudad Autónoma de Buenos Aires",
    "OCASA": "La Rioja 301, Ciudad Autónoma de Buenos Aires",
    "Cruz del Sur (Transportes)": "Av. Del Libertador 5478, Ciudad Autónoma de Buenos Aires",
    "TASA Logística": "Dardo Rocha 2934, Martinez, Buenos Aires",
    "Murchison S.A.": "Av. Ramón Castillo int. R. Obligado, Ciudad Autónoma de Buenos Aires",
    "Federal Express Corporation (FedEx)": "Av. Cabildo 1559, Ciudad Autónoma de Buenos Aires",
    "Schenker Argentina S.A.": "Tucuman 117, Ciudad Autónoma de Buenos Aires",
    "Hellmann Worldwide Logistics S.A.": "Tucuman 1, Ciudad Autónoma de Buenos Aires", # Generalmente cerca de Schenker en puerto
    "TransFarmaco S.A.": "Ruta 9 KM 37.5, Benavidez, Buenos Aires",
    "Calico S.A.": "Poeta Risso 2745, Hurlingham, Buenos Aires",
    "OCA Logística": "La Rioja 301, Ciudad Autónoma de Buenos Aires",
    "La Sevillanita": "Pergamino 3751, Ciudad Autónoma de Buenos Aires",
    "Expreso Oro Negro": "Av. General Iriarte 3911, Ciudad Autónoma de Buenos Aires",
    "Raosa Transportes": "Av. General Iriarte 3500, Ciudad Autónoma de Buenos Aires",
    "Transportes Snaider": "Av. General Iriarte 3600, Ciudad Autónoma de Buenos Aires",
    "Expreso Malargue": "Av. General Iriarte 3400, Ciudad Autónoma de Buenos Aires"
}

def normalizar_direccion(direccion_texto):
    try:
        url = "https://apis.datos.gob.ar/georef/api/direcciones"
        params = {"direccion": direccion_texto, "max": 1}
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        if data["direcciones"]:
            res = data["direcciones"][0]
            # Retornamos la dirección normalizada completa
            return res.get("nom_completa", direccion_texto)
    except Exception as e:
        print(f"Error normalizando {direccion_texto}: {e}")
    return direccion_texto

def update_logisticas():
    tel = "1165856635"
    email = "marcelo_peri@yahoo.com"
    
    with get_db_cursor() as cursor:
        print("--- Actualizando Datos de Logística y Direcciones ---")
        for nombre_db, direccion_raw in DIRECCIONES_RAW.items():
            print(f"Procesando: {nombre_db}...")
            
            # 1. Normalizar dirección con Georef
            dir_normalizada = normalizar_direccion(direccion_raw)
            
            # 2. Actualizar tabla stk_logisticas
            # Nota: Agregamos la dirección concatenada al nombre o si hubiera un campo domicilio (stk_logisticas no tenía campo dirección pero lo inferimos o actualizamos el nombre)
            # Reviso si existen campos de dirección. stk_logisticas tenía id, enterprise_id, nombre, cuit, telefono, email, activo, created_at.
            # No tiene campo de dirección físico por defecto, así que actualizaremos tel y email principalmente.
            
            cursor.execute("""
                UPDATE stk_logisticas 
                SET telefono = %s, email = %s
                WHERE nombre LIKE %s OR nombre = %s
            """, (tel, email, f"%{nombre_db}%", nombre_db))
            
            if cursor.rowcount > 0:
                print(f"   [OK] {nombre_db} actualizado con Tel: {tel} e Email: {email}")
            else:
                print(f"   [WARN] No se encontró {nombre_db} en la base de datos.")
        
        # Caso especial: Transporte Propio (Cuit 0 o similar)
        cursor.execute("UPDATE stk_logisticas SET telefono = %s, email = %s WHERE enterprise_id = %s", (tel, email, 0))
        
        print("--- Proceso Finalizado ---")

if __name__ == "__main__":
    update_logisticas()
