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

def fix_horrors():
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
                        
                        # 1. Fix _await and variations like q_await, etc.
                        # Pattern matches any letters/underscores immediately followed by await
                        # But not 'await' itself as a word.
                        content = re.sub(r'\b[\w_]*_await\s+', 'await ', content)
                        
                        # 2. Fix the parenthesis await corruption
                        content = re.sub(r'\(await\s+request\.files\)await\s+\[', r'await (await request.files)[', content)
                        
                        # 3. Fix any obj'await ]
                        content = re.sub(r"(['\"][\w_]+['\"])await\s+\]", r"\1]", content)

                        # 4. Fix general .await
                        content = re.sub(r'([\w\d\.\[\]\(\)\'\"]+)\.await\s+(\w+)\(', r'await \1.\2(', content)

                        if content != original:
                            with open(os.path.join(root, file), 'w', encoding='utf-8') as f:
                                f.write(content)
                            print(f"Horror fijado: {file}")
                            count += 1
                    except: pass
    print(f"Total horrores fijados: {count}")

if __name__ == "__main__":
    fix_horrors()
