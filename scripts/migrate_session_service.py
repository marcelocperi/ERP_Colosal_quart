import re
import os

def migrate_session_service_to_quart():
    filepath = r'C:\Users\marce\Documents\GitHub\quart\services\session_service.py'
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Update imports
    content = content.replace('from flask import', 'from quart import')
    
    # 2. Make methods async
    content = content.replace('def get_current_sid():', 'async def get_current_sid():')
    content = content.replace('def attach_session_context():', 'async def attach_session_context():')
    
    # 3. Await request.form and other awaitables
    # Prioridad 2: sid = request.args.get('sid') or request.form.get('sid') or request.headers.get('X-SID')
    # Change to: sid = request.args.get('sid') or (await request.form).get('sid') or request.headers.get('X-SID')
    content = content.replace("request.form.get('sid')", "(await request.form).get('sid')")
    
    # 4. Await calls to these methods
    content = content.replace('sid = SessionDispatcher.get_current_sid()', 'sid = await SessionDispatcher.get_current_sid()')
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Migración base de services/session_service.py a Quart completada.")

if __name__ == "__main__":
    migrate_session_service_to_quart()
