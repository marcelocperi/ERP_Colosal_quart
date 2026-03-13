import os
import sys
import json
import asyncio

project_root = os.getcwd()
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

async def run_query(sql, params=None):
    try:
        async with get_db_cursor(dictionary=True) as cursor:
            await cursor.execute(sql, params or ())
            # Intentar obtener resultados solo si es una consulta que los devuelve
            if cursor.description:
                results = await cursor.fetchall()
                print(json.dumps(results, indent=2, default=str))
            else:
                print(json.dumps({"status": "success", "rowcount": cursor.rowcount}))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python db_cli.py \"SQL QUERY\" [json_params]")
    else:
        sql = sys.argv[1]
        params = json.loads(sys.argv[2]) if len(sys.argv) > 2 else None
        asyncio.run(run_query(sql, params))
