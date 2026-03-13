import os
import re

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

def fix_registrations():
    count = 0
    for tdir in TARGET_DIRS:
        if not os.path.exists(tdir): continue
        for root, _, files in os.walk(tdir):
            for file in files:
                if file.endswith('.py'):
                    try:
                        with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        original = content
                        
                        # Fix ANY register_ function being awaited at top-level OR defined as async
                        content = re.sub(r'async def (register_\w+)\(', r'def \1(', content)
                        content = re.sub(r'await\s+(register_\w+)\(', r'\1(', content)

                        if content != original:
                            with open(os.path.join(root, file), 'w', encoding='utf-8') as f:
                                f.write(content)
                            print(f"Registro fijado: {file}")
                            count += 1
                    except: pass
    print(f"Total registros fijados: {count}")

if __name__ == "__main__":
    fix_registrations()
