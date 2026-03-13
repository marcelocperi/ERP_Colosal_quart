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

def clean_async_errors():
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
                        
                        # 1. async with await -> async with
                        content = re.sub(r'async with await (?!open)\b([\w\.]+)\(', 
                                            r'async with \1(', 
                                            content)
                        
                        # 2. async async -> async
                        content = re.sub(r'async\s+async\s+', 'async ', content)
                        
                        # 3. await await -> await
                        content = re.sub(r'await\s+await\s+', 'await ', content)
                        
                        # 4. obj.await func() -> await obj.func()
                        content = re.sub(r'([\w\.]+)\.await\s+(\w+)\(', r'await \1.\2(', content)

                        if content != original:
                            with open(os.path.join(root, file), 'w', encoding='utf-8') as f:
                                f.write(content)
                            print(f"Limpiado: {file}")
                            count += 1
                    except: pass
    print(f"Total archivos limpiados: {count}")

if __name__ == "__main__":
    clean_async_errors()
