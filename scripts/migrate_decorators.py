import re
import os

def migrate_decorators_to_quart():
    filepath = r'C:\Users\marce\Documents\GitHub\quart\core\decorators.py'
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Update imports
    content = content.replace('from flask import', 'from quart import')
    
    # 2. Make _unauthorized_response async because it uses awaitable request properties
    content = content.replace('def _unauthorized_response(message=', 'async def _unauthorized_response(message=')
    
    # 3. Await request properties in _unauthorized_response
    content = content.replace('if request.is_json: req_data = request.json', 'if request.is_json: req_data = await request.json')
    content = content.replace('elif request.form: req_data = dict(request.form)', 'elif (await request.form): req_data = dict(await request.form)')
    content = content.replace('request.form.get(', '(await request.form).get(')
    
    # 4. Make login_required properly async for Quart
    # Quart's @login_required (if we were using a library) works differently, but manually:
    # We should ensure the wrapped_view awaits _unauthorized_response
    content = content.replace('return _unauthorized_response()', 'return await _unauthorized_response()')
    
    # 5. Make _log_forbidden_try async
    content = content.replace('def _log_forbidden_try(', 'async def _log_forbidden_try(')
    content = content.replace('if request.is_json: req_data = request.json', 'if request.is_json: req_data = await request.json')
    content = content.replace('elif request.form: req_data = dict(request.form)', 'elif (await request.form): req_data = dict(await request.form)')
    
    # 6. Await calls to _log_forbidden_try and flash
    content = content.replace('_log_forbidden_try(', 'await _log_forbidden_try(')
    content = content.replace('flash(', 'await flash(')

    # 7. Final check on the login_required and permission_required logic.
    # We want to force async wrappers since Quart routes are async.
    # The current logic uses inspect.iscoroutinefunction(view). 
    # In Quart, view is almost always a coroutine function.
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Migración base de core/decorators.py a Quart completada.")

if __name__ == "__main__":
    migrate_decorators_to_quart()
