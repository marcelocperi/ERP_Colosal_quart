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

def final_polish():
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
                        
                        # 1. asyncio.run(...) cleanup
                        # res = asyncio.run(await func(...)) -> res = await func(...)
                        # res = asyncio.run(func(...)) -> res = await func(...) if func is in an async def
                        content = re.sub(r'asyncio\.run\(await\s+(.*?)\)', r'await \1', content)
                        content = re.sub(r'asyncio\.run\((.*?)\)', r'await \1', content)
                        
                        # 2. Redundancia de await await
                        content = re.sub(r'await\s+await\s+', 'await ', content)
                        
                        # 3. Fix double await in return statements (return await await render_template)
                        content = re.sub(r'return\s+await\s+await\s+', 'return await ', content)

                        # 4. Remove 'import asyncio' if it was lonely after our fixes (Optional, but cleaner)
                        # We won't risk it for now.

                        if content != original:
                            with open(os.path.join(root, file), 'w', encoding='utf-8') as f:
                                f.write(content)
                            print(f"Pulido final: {file}")
                            count += 1
                    except: pass
    print(f"Total archivos pulidos: {count}")

if __name__ == "__main__":
    final_polish()
