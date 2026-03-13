import re
import os

def patch_core_awaits():
    filepath = r'C:\Users\marce\Documents\GitHub\quart\core\routes.py'
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Await a _log_security_event
    content = re.sub(r'(?<!await\s)_log_security_event\(', 'await _log_security_event(', content)
    
    # 2. Await a make_response
    content = re.sub(r'(?<!await\s)make_response\(', 'await make_response(', content)
    
    # 3. Await a flash (re-check)
    content = re.sub(r'(?<!await\s)flash\(', 'await flash(', content)

    # 4. Await render_template (re-check)
    content = re.sub(r'(?<!await\s)render_template\(', 'await render_template(', content)

    # 5. Await request properties (re-check)
    content = re.sub(r'(?<!await\s)request\.form\.get\(', '(await request.form).get(', content)
    content = re.sub(r'(?<!await\s)request\.json', '(await request.json)', content)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Core routes patched with missing awaits.")

if __name__ == "__main__":
    patch_core_awaits()
