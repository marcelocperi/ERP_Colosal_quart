import re
import os

def migrate_app_to_quart():
    filepath = r'C:\Users\marce\Documents\GitHub\quart\app.py'
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Imports and definitions
    content = content.replace('from flask import Flask, session, g, request, url_for', 'from quart import Quart, session, g, request, url_for')
    content = content.replace('from flask.json.provider import DefaultJSONProvider', 'from quart.json.provider import DefaultJSONProvider')
    content = content.replace('app = Flask(__name__)', 'app = Quart(__name__)')

    # Add awaiting to form fields
    content = content.replace('request.form.get(', '(await request.form).get(')
    
    # Check jsonify import
    content = content.replace('from flask import jsonify', 'from quart import jsonify')

    # Apply async to handlers
    content = content.replace('def security_and_auth():', 'async def security_and_auth():')
    content = content.replace('def add_header(response):', 'async def add_header(response):')
    content = content.replace('def add_security_headers(response):', 'async def add_security_headers(response):')
    content = content.replace('def inject_globals():', 'async def inject_globals():')
    content = content.replace('def global_exception_handler(e):', 'async def global_exception_handler(e):')

    # Await in json/form checks inside error handler
    content = content.replace(
        'if request.is_json: req_data = request.json\n            elif request.form: req_data = dict(request.form)',
        'if request.is_json: req_data = await request.json\n            elif await request.form: req_data = dict(await request.form)'
    )

    # Server configuration logic
    hypercorn_str = '''        try:
            from hypercorn.config import Config as HyperConfig
            from hypercorn.asyncio import serve
            import asyncio
            print(f"MODO: PRODUCCION (Hypercorn Server)")
            print(f"URL:  http://0.0.0.0:{port}")
            print(f"Acceso Externo: Configura tu router puerto {port} -> 192.168.0.97")
            print("="*60)
            
            config = HyperConfig()
            config.bind = [f"0.0.0.0:{port}"]
            asyncio.run(serve(app, config))
        except ImportError:
            print("[ALERTA] Hypercorn no instalado. Usando Quart nativo.")
            app.run(host='0.0.0.0', port=port, debug=False)'''

    waitress_str_regex = re.compile(r'        try:\n            from waitress import serve.*?app\.run\(host=\'0\.0\.0\.0\', port=port, debug=False\)', re.DOTALL)
    content = waitress_str_regex.sub(hypercorn_str, content)

    content = content.replace('MODO: DESARROLLO (Flask Debug)', 'MODO: DESARROLLO (Quart Debug)')

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Migración base de app.py a Quart completada.")

if __name__ == "__main__":
    migrate_app_to_quart()
