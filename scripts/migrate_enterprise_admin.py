import re
import os

def migrate_enterprise_admin_to_quart():
    filepath = r'C:\Users\marce\Documents\GitHub\quart\core\enterprise_admin.py'
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Update imports
    content = content.replace('from flask import', 'from quart import')
    
    # 2. Make routes async
    lines = content.split('\n')
    inside_route = False
    new_lines = []
    for line in lines:
        if line.strip().startswith('@ent_bp.route') or line.strip().startswith('@login_required') or line.strip().startswith('@permission_required') or line.strip().startswith('@atomic_transaction'):
            inside_route = True
        elif line.strip().startswith('def ') and inside_route:
            line = line.replace('def ', 'async def ', 1)
            inside_route = False
        elif line.strip() != '' and not line.strip().startswith('#') and not line.strip().startswith('@'):
            inside_route = False
        new_lines.append(line)
    content = '\n'.join(new_lines)

    # 3. Await flash, render_template, make_response
    content = re.sub(r'flash\(', r'await flash(', content)
    content = re.sub(r'render_template\(', r'await render_template(', content)
    content = re.sub(r'make_response\(', r'await make_response(', content)

    # 4. Await request properties
    content = re.sub(r'request\.form\.get\(', r'(await request.form).get(', content)
    content = re.sub(r'request\.form\.getlist\(', r'(await request.form).getlist(', content)
    content = re.sub(r'request\.json', r'(await request.json)', content)
    content = re.sub(r'request\.files', r'(await request.files)', content)
    
    # Special: request.json.get -> (await request.json).get
    # Since I did a global replace for request.json, I need to check if I broke anything.
    # Actually, (await request.json) is safe.
    
    # Handle request.files['logo'] or similar
    # (await request.files)['logo']
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Migración base de core/enterprise_admin.py a Quart completada.")

if __name__ == "__main__":
    migrate_enterprise_admin_to_quart()
