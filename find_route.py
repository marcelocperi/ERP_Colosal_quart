
with open(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP\biblioteca\routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        if '/prestamos' in line and '@' in line:
            print(f"{i+1}: {line.strip()}")
        if 'def prestamos' in line:
            print(f"{i+1}: {line.strip()}")
