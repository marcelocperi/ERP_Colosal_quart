import os
import re

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

def fix_syntax_errors():
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
                        
                        # 1. @await decorator -> @decorator
                        content = re.sub(r'@await\s+', '@', content)
                        
                        # 2. obj.await func() -> await obj.func()
                        # Maneja casos como files['a'].await read() -> await files['a'].read()
                        content = re.sub(r'([\w\d\.\]\)]+)\.await\s+(\w+)\(', r'await \1.\2(', content)

                        # 3. await await func( -> await func( (Double awaits)
                        content = re.sub(r'await\s+await\s+', 'await ', content)
                        
                        # 4. Correct duplicated request await patterns
                        content = re.sub(r'\(await\s+\(await\s+request\.(\w+)\)\)', r'(await request.\1)', content)

                        # 5. with await -> async with
                        content = re.sub(r'\bwith\s+await\s+', 'async with ', content)
                        
                        # 6. if __name__ == "__main__": async_func() -> asyncio.run(async_func())
                        if 'app.py' not in file and 'scripts' not in root:
                            content = re.sub(r'if\s+__name__\s*==\s*"__main__"\s*:\s*\n\s+([\w\.]+)\(\)', 
                                             r'if __name__ == "__main__":\n    import asyncio\n    asyncio.run(\1())', content)

                        if content != original:
                            with open(os.path.join(root, file), 'w', encoding='utf-8') as f:
                                f.write(content)
                            print(f"Sintaxis corregida: {file}")
                            count += 1
                    except: pass
    print(f"Total archivos corregidos: {count}")

if __name__ == "__main__":
    fix_syntax_errors()
