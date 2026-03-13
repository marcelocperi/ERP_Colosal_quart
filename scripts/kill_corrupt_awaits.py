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

def kill_corrupt_awaits():
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
                        
                        # 1. Fix 'sessionawait .' -> 'session.'
                        content = re.sub(r'(\w+)await\s*\.', r'\1.', content)
                        
                        # 2. Fix 'await .await' -> 'await' (o dejar solo el punto si era necesario)
                        content = re.sub(r'await\s*\.\s*await', r'await', content)

                        # 3. Fix cases like 'await await' or triple 'await'
                        content = re.sub(r'\bawait\s+await\b', 'await', content)
                        content = re.sub(r'\bawait\s+await\b', 'await', content)

                        if content != original:
                            with open(os.path.join(root, file), 'w', encoding='utf-8') as f:
                                f.write(content)
                            print(f"Limpia de corrupción: {file}")
                            count += 1
                    except: pass
    print(f"Total archivos saneados: {count}")

if __name__ == "__main__":
    kill_corrupt_awaits()
