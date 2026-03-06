
import os
import re

def find_async_functions(root_dir):
    async_funcs = {}
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    matches = re.findall(r'async def\s+(\w+)', content)
                    if matches:
                        async_funcs[path] = matches
    return async_funcs

def find_calls_without_await(root_dir, async_funcs_list):
    results = []
    # Collapse all func names into a set
    all_async_names = set()
    for funcs in async_funcs.values():
        all_async_names.update(funcs)
    
    # We also know AfipService methods are async
    all_async_names.update(['consultar_padron', 'solicitar_cae', '_obtener_login_ticket'])

    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    for i, line in enumerate(lines):
                        for func in all_async_names:
                            # Search for "func(" but without "await" on the same line (approximate)
                            if re.search(rf'(?<!await\s){re.escape(func)}\(', line) and not line.strip().startswith('async def'):
                                # Also exclude class definitions or calls where it IS awaited but maybe not on the same line?
                                # This is a heuristic.
                                results.append(f"{path}:{i+1}: {line.strip()}")
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
