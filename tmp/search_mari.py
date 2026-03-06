import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def search_mari():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, razon_social, nombre, es_cliente FROM erp_terceros WHERE razon_social LIKE '%MARI%' OR nombre LIKE '%MARI%'")
        results = cursor.fetchall()
        for r in results:
            print(f"ID: {r['id']}, Razon: {r['razon_social']}, Nombre: {r['nombre']}, IsClient: {r['es_cliente']}")

if __name__ == "__main__":
    search_mari()
