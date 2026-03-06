import os
from dotenv import load_dotenv
import json
import traceback
import sys

# Cargar entorno
load_dotenv()

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

# Forzar encoding UTF-8 para evitar errores de charmap en Windows
sys.stdout.reconfigure(encoding='utf-8')

try:
    from database import get_db_cursor
    
    def get_schema(table):
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute(f"DESCRIBE {table}")
            return cursor.fetchall()

    if __name__ == "__main__":
        table_name = "erp_terceros"
        if len(sys.argv) > 1:
            table_name = sys.argv[1]
            
        schema = get_schema(table_name)
        # Usar json.dump directamente al archivo para evitar problemas de consola si es necesario
        with open(f"tmp/{table_name}_schema.json", "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, default=str)
        print(f"Schema saved to tmp/{table_name}_schema.json")

except Exception as e:
    print(f"Error: {e}")
    traceback.print_exc()
