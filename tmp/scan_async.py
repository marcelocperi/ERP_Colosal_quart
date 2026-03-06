import sys, os
sys.path.insert(0, r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')

route_files = [
    r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\ventas\routes.py',
    r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\contabilidad\routes.py',
    r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\compras\routes.py',
    r'c:\Users\marce\Documents\GitHub\bibliotecaweb\GitHub\bibliotecaweb\multiMCP\core\routes.py',
]

route_files = [
    r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\ventas\routes.py',
    r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\contabilidad\routes.py',
    r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\compras\routes.py',
    r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\core\routes.py',
]

print("=== SCAN: async def / await in route files ===\n")
for path in route_files:
    fname = os.path.basename(os.path.dirname(path)) + '/' + os.path.basename(path)
    found = []
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f, 1):
                stripped = line.strip()
                if 'async def' in stripped or (stripped.startswith('await ') or ' await ' in stripped or '= await ' in stripped):
                    found.append((i, line.rstrip()))
    except Exception as e:
        print(f"  ERROR reading {fname}: {e}")
        continue
    if found:
        print(f"[!] {fname}:")
        for lineno, content in found:
            print(f"    L{lineno}: {content}")
    else:
        print(f"[OK] {fname}: clean")
