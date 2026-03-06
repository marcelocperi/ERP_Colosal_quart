
import os
import re

async_def_pattern = re.compile(r'async\s+def\s+([a-zA-Z0-9_]+)\s*\(')

# Find all async function names in the project
async_functions = set()
for root, dirs, files in os.walk(os.getcwd()):
    if 'venv' in root or '.git' in root or '__pycache__' in root:
        continue
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                for match in async_def_pattern.finditer(content):
                    async_functions.add(match.group(1))

print(f"Found {len(async_functions)} async functions.")

# Pattern to find calls to these functions without await
# This is tricky because of false positives, but we can look for "function_name(" not preceded by "await "
# and within a synchronous "def" (non-async)
def_pattern = re.compile(r'(?m)^(async\s+)?def\s+([a-zA-Z0-9_]+)\s*\(.*?\):([\s\S]*?)(?=(?m)^async\s+def|^def|\Z)')

results = []

for root, dirs, files in os.walk(os.getcwd()):
    if 'venv' in root or '.git' in root or '__pycache__' in root:
        continue
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            for match in def_pattern.finditer(content):
                is_async_def = match.group(1) is not None
                func_name = match.group(2)
                body = match.group(3)
                
                # If it's a synchronous def, check if it calls any async functions
                if not is_async_def:
                    for async_func in async_functions:
                        # Basic check: async_func( is in body and not preceded by await
                        # We also check if it's imported or available
                        if f"{async_func}(" in body and f"await {async_func}(" not in body:
                            # Verify if it's a real call and not just a string/comment
                            # (Omitted for brevity in this scratch script, will manual check results)
                            line_no = content.count('\n', 0, match.start() + body.find(async_func)) + 1
                            results.append(f"{filepath}:{line_no} Sync def '{func_name}' calls async '{async_func}' without await.")

for res in results:
    print(res)
