import os
import re
import time

# Todos los directorios principales del proyecto
TARGET_DIRS = [
    r'C:\Users\marce\Documents\GitHub\quart\core',
    r'C:\Users\marce\Documents\GitHub\quart\services',
    r'C:\Users\marce\Documents\GitHub\quart\ventas',
    r'C:\Users\marce\Documents\GitHub\quart\compras',
    r'C:\Users\marce\Documents\GitHub\quart\stock',
    r'C:\Users\marce\Documents\GitHub\quart\contabilidad',
    r'C:\Users\marce\Documents\GitHub\quart\fondos',
    r'C:\Users\marce\Documents\GitHub\quart\utilitarios',
    r'C:\Users\marce\Documents\GitHub\quart\pricing',
    r'C:\Users\marce\Documents\GitHub\quart\produccion',
    r'C:\Users\marce\Documents\GitHub\quart\utils',
    r'C:\Users\marce\Documents\GitHub\quart\tasks',
    r'C:\Users\marce\Documents\GitHub\quart\repositories'
]

# Funciones conocidas asíncronas o por defecto del driver/framework
ASYNC_FUNCTIONS = {
    'get_db_cursor', 'atomic_transaction', 'get_db_pool', '_log_transaction_error',
    '_log_security_event', 'get_enterprise_email_config', '_obtener_branding',
    '_enviar_email', 'flash', 'render_template', 'make_response', 'jsonify',
    'initialize_enterprise_master_data', 'consultar_padron', 'get_allowed_comprobantes',
    'execute', 'fetchall', 'fetchone', 'commit', 'rollback', 'read', 'save', 'send_from_directory'
}

def scan_async_funcs():
    async_funcs = set(ASYNC_FUNCTIONS)
    for tdir in TARGET_DIRS:
        if not os.path.exists(tdir): continue
        for root, _, files in os.walk(tdir):
            for file in files:
                if file.endswith('.py'):
                    try:
                        with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                            content = f.read()
                            found = re.findall(r'async def (\w+)', content)
                            for f_name in found:
                                if not f_name.startswith('register_'):
                                    async_funcs.add(f_name)
                    except: pass
    return async_funcs

def convert_to_async_def(content):
    lines = content.split('\n')
    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        match = re.match(r'^(\s*)(?:async\s+)?def\s+(\w+)\(.*\):', line)
        if match:
            indent_str = match.group(1)
            func_name = match.group(2)
            
            # FORCE SYNC for decorators and route registration
            if func_name in ['login_required', 'permission_required', 'atomic_transaction', 'decorator'] or func_name.startswith('register_'):
                # Si es async, lo hacemos sync
                if line.strip().startswith('async '):
                    line = line.replace('async def ', 'def ', 1)
                new_lines.append(line)
                i += 1
                continue

            start_i = i
            indent = len(indent_str)
            has_await_at_this_level = False
            i += 1
            while i < len(lines):
                subline = lines[i]
                if subline.strip() == "":
                    i += 1
                    continue
                sub_indent = len(subline) - len(subline.lstrip())
                if sub_indent <= indent:
                    break
                
                if re.match(rf'^\s{{{indent+1},}}(?:async\s+)?def\s+\w+', subline):
                    nested_indent = sub_indent
                    i += 1
                    while i < len(lines):
                        inner_subline = lines[i]
                        if inner_subline.strip() == "":
                            i += 1
                            continue
                        if (len(inner_subline) - len(inner_subline.lstrip())) <= nested_indent:
                            break
                        i += 1
                    continue

                if 'await ' in subline:
                    has_await_at_this_level = True
                i += 1
            
            current_header = lines[start_i]
            if has_await_at_this_level and not current_header.strip().startswith('async '):
                lines[start_i] = current_header.replace('def ', 'async def ', 1)
            
            for j in range(start_i, i):
                new_lines.append(lines[j])
            continue
        else:
            new_lines.append(line)
        i += 1
    return '\n'.join(new_lines)

def propagate_async(filepath, async_funcs):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original = content
        
        # 1. Add await to recognized calls
        filtered_funcs = [f for f in async_funcs if not f.startswith('register_')]
        pattern = re.compile(rf'(?<!await\s)(?<!def\s)(?<!async\s def\s)\b([\w\d\.\]\)]+\.)?(%s)\s*\(' % '|'.join(filtered_funcs))
        content = pattern.sub(r'await \1\2(', content)

        # 2. Special for request properties de Quart
        props = ['form', 'json', 'files', 'data']
        for prop in props:
            content = re.sub(rf'(?<!await\s)request\.{prop}\b', f'await request.{prop}', content)
        
        # 3. Convert def to async def if there's an await inside
        content = convert_to_async_def(content)
        
        if content != original:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
    except Exception as e:
        print(f"Error procesando {filepath}: {e}")
    return False

def run_propagation():
    start_time = time.time()
    async_funcs = scan_async_funcs()
    print(f"Detectadas {len(async_funcs)} funciones asíncronas.")
    
    for iteration in range(1, 5):
        print(f"Iteración {iteration}...")
        files_changed = 0
        for tdir in TARGET_DIRS:
            if not os.path.exists(tdir): continue
            for root, _, files in os.walk(tdir):
                for file in files:
                    if file.endswith('.py') and file != 'propagate_async.py':
                        if propagate_async(os.path.join(root, file), async_funcs):
                            files_changed += 1
        
        if files_changed == 0:
            print("No se encontraron más cambios.")
            break
        print(f"Archivos actualizados: {files_changed}")
        async_funcs = scan_async_funcs()

    print(f"Propagación finalizada en {time.time() - start_time:.2f} segundos.")

if __name__ == "__main__":
    run_propagation()
