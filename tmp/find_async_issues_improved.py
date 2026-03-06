
import os
import re

def find_async_functions(root_dir):
    async_funcs = {}
    for root, _, files in os.walk(root_dir):
        if any(x in root for x in ['venv', '.git', '.gemini', '__pycache__']):
            continue
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        matches = re.findall(r'async def\s+(\w+)', content)
                        if matches:
                            async_funcs[path] = matches
                except: pass
    return async_funcs

def find_calls_without_await(root_dir, async_funcs):
    results = []
    all_async_names = set()
    for funcs in async_funcs.values():
        all_async_names.update(funcs)
    
    # Add known async AfipService methods if not found
    all_async_names.update(['consultar_padron', 'solicitar_cae', '_obtener_login_ticket'])

    for root, _, files in os.walk(root_dir):
        if any(x in root for x in ['venv', '.git', '.gemini', '__pycache__']):
            continue
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()
                        for i, line in enumerate(lines):
                            for func in all_async_names:
                                # Look for func() call
                                if f"{func}(" in line:
                                    # If it's a call, check if 'await' is present
                                    if 'await' not in line and 'async def' not in line:
                                        # Simple check to avoid comments/strings
                                        if line.strip().startswith('#'): continue
                                        results.append(f"{path}:{i+1}: {line.strip()}")
                                        break
                except: pass
    return results

root = '.'
async_funcs = find_async_functions(root)
print("--- ASYNC FUNCTIONS FOUND ---")
for path, funcs in async_funcs.items():
    print(f"{path}: {funcs}")

print("\n--- POTENTIAL MISSING AWAITS ---")
calls = find_calls_without_await(root, async_funcs)
for c in calls:
    print(c)
