import os
import re

TARGET_DIRS = [
    r'C:\Users\marce\Documents\GitHub\quart\core',
    r'C:\Users\marce\Documents\GitHub\quart\services'
]

def fix_async_files():
    count = 0
    for tdir in TARGET_DIRS:
        for root, _, files in os.walk(tdir):
            for file in files:
                if file.endswith('.py'):
                    try:
                        with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        original = content
                        
                        # 1. request.files -> (await request.files)
                        # Evitar duplicar await
                        content = re.sub(r'(?<!await\s)request\.files', r'(await request.files)', content)
                        
                        # 2. obj.read() -> await obj.read()
                        # Solo si obj parece ser un archivo de request o algo así
                        # Heurística: si está cerca de (await request.files) o es una variable común como logo_file
                        # Buscamos patrones como: logo_file.read() -> await logo_file.read()
                        # O simplemente cualquier .read() que no esté ya awaited
                        # .read() / .save() / .seek()
                        
                        content = re.sub(r'(?<!await\s)(\w+)\.read\(', r'await \1.read(', content)
                        content = re.sub(r'(?<!await\s)(\w+)\.save\(', r'await \1.save(', content)
                        content = re.sub(r'(?<!await\s)(\w+)\.seek\(', r'await \1.seek(', content)

                        if content != original:
                            with open(os.path.join(root, file), 'w', encoding='utf-8') as f:
                                f.write(content)
                            print(f"File handling fixed: {file}")
                            count += 1
                    except: pass
    print(f"Total archivos corregidos: {count}")

if __name__ == "__main__":
    fix_async_files()
