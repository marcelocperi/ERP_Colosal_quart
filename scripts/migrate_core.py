import re
import os

def migrate_core_routes():
    filepath = r'C:\Users\marce\Documents\GitHub\quart\core\routes.py'
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Reemplazar imports de Flask por Quart
    content = content.replace('from flask import', 'from quart import')
    content = content.replace('from quart import Blueprint, render_template, request, redirect, url_for, flash, session, g, jsonify, current_app', 
                              'from quart import Blueprint, render_template, request, redirect, url_for, flash, session, g, jsonify, current_app, make_response')

    # 2. Hacer asíncronas las funciones con decoradores de rutas de core_bp
    # Busca @core_bp.route o métodos como .get/.post, seguido de posibles otros decoradores, seguido de def
    # Expresión regular: encuentra 'def nombre_funcion(' asegurándose que antes haya un route.
    # Dado que los archivos varían, es más seguro buscar todas las "def " que tienen un @route o @login_required encima.
    
    # Expresión un poco sucia pero efectiva para Python scripts: reemplazar "def " por "async def " si abajo es una ruta.
    # En Quart *todas* las rutas deben ser asíncronas.
    lines = content.split('\n')
    inside_route = False
    new_lines = []
    
    for i, line in enumerate(lines):
        # Si es un decorador de ruta, preparamos la bandera
        if line.strip().startswith('@core_bp.') or line.strip().startswith('@login_required') or line.strip().startswith('@permission_required') or line.strip().startswith('@atomic_transaction'):
            inside_route = True
        
        # Si encontramos def y estamos en una ruta
        if line.strip().startswith('def ') and inside_route:
            line = line.replace('def ', 'async def ', 1)
            inside_route = False # Reset flag
        
        # Si hay una línea vacía o no relacionada pero no era un def (ej comentarios), no reseteamos inmediatamente, pero el approach es heurístico.
        if line.strip() == '' or line.strip().startswith('#'):
            pass # keep inside_route state
        elif not line.strip().startswith('@') and not line.strip().startswith('async def'):
            inside_route = False
            
        new_lines.append(line)
        
    content = '\n'.join(new_lines)
    
    # 3. await flash()
    content = re.sub(r'flash\(', r'await flash(', content)
    
    # 4. await request.form
    # Cuidado con diccionarios: request.form['key'] -> (await request.form)['key']
    content = re.sub(r'request\.form\.get\(', r'(await request.form).get(', content)
    content = re.sub(r'request\.form\[', r'(await request.form)[', content)
    
    # 5. await request.json
    content = re.sub(r'request\.json\.get\(', r'(await request.json).get(', content)
    content = re.sub(r'request\.json\[', r'(await request.json)[', content)
    
    # 6. await render_template()
    content = re.sub(r'render_template\(', r'await render_template(', content)

    # 7. request.method no necesita await, urls redirigidas tampoco.
    # Pero si hay request.data puede necesitarlo.
    content = re.sub(r'request\.get_json\(\)', r'await request.get_json()', content)

    # Convertir _async_email en async background task standard para Quart (opcional, por ahora lo dejamos con threads u omitimos. Mejor threads)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Migración base de core/routes.py a Quart completada.")

if __name__ == "__main__":
    migrate_core_routes()
