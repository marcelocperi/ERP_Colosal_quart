from database import get_db_cursor
import re

def sync_codes_back():
    try:
        with get_db_cursor(dictionary=True) as cursor:
            print("Sincronizando códigos con tablas originales...")
            
            # Obtener todos los códigos de erp_terceros
            cursor.execute("SELECT cuit, codigo, es_proveedor, es_cliente FROM erp_terceros")
            terceros = cursor.fetchall()

            for t in terceros:
                if t['es_proveedor']:
                    cursor.execute("UPDATE proveedores SET codigo = %s WHERE cuit = %s", (t['codigo'], t['cuit']))
                if t['es_cliente']:
                    cursor.execute("UPDATE clientes SET codigo = %s WHERE cuit = %s", (t['codigo'], t['cuit']))

            print("Sincronización completada.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    sync_codes_back()
