import requests
from database import get_db_cursor

def obtener_y_guardar_cotizacion(enterprise_id, origen='automatico'):
    """Obtiene la cotización del dolar de la API oficial y la guarda en la BD vinculada a una empresa."""
    if enterprise_id is None:
        print("Error Finanzas: enterprise_id es requerido")
        return None
        
    url = "https://dolarapi.com/v1/dolares/oficial"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO cotizacion_dolar (enterprise_id, compra, venta, casa, nombre, moneda, fechaActualizacion, origen)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (enterprise_id, data['compra'], data['venta'], data['casa'], data['nombre'], data['moneda'], data['fechaActualizacion'], origen))
        
        return data
    except Exception as e:
        print(f"Error en Finanzas: {e}")
        return None
