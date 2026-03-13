import os
import re

TARGET_DIRS = [
    r'C:\Users\marce\Documents\GitHub\quart\core',
    r'C:\Users\marce\Documents\GitHub\quart\services'
]

def clean_sync_awaits():
    count = 0
    for tdir in TARGET_DIRS:
        for root, _, files in os.walk(tdir):
            for file in files:
                if file.endswith('.py'):
                    try:
                        with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        original = content
                        
                        # Fix: with open(...) as f: ... await f.read() -> f.read()
                        # Buscamos el patrón de 'with open' y capturamos el nombre de la variable
                        
                        # 1. Encontrar el nombre del archivo en 'with open(...) as NAME:'
                        # Usamos regex multilínea para buscar el cuerpo
                        
                        # Heurística simple: si vemos un 'with open' seguido de un 'await NAME.read()' dentro del mismo bloque
                        # O simplemente, si es 'f.read()' y 'f' es un nombre común de archivo síncrono.
                        # Mejor aún: buscamos lineas que digan 'await NAME.read()' y vemos si NAME fue definido en un 'with open'.
                        
                        # Pero para hacerlo mas seguro y rápido, simplemente corregimos los nombres comunes que suelen ser síncronos
                        # como 'f', 'key_file', 'part_file', etc.
                        
                        # O mejor: buscar pares de 'with open' y corregir el await interior.
                        
                        def fix_sync_block(m):
                            block = m.group(0)
                            name = m.group(1)
                            # Reemplazar await name.read() -> name.read()
                            return re.sub(rf'await {name}\.(read|write|save|seek)\(', rf'{name}.\1(', block)

                        content = re.sub(r'with open\(.*?\)\s+as\s+(\w+):.*?(?:\n\s+.*)*', fix_sync_block, content)

                        if content != original:
                            with open(os.path.join(root, file), 'w', encoding='utf-8') as f:
                                f.write(content)
                            print(f"Sync awaits fixed: {file}")
                            count += 1
                    except: pass
    print(f"Total archivos corregidos: {count}")

if __name__ == "__main__":
    clean_sync_awaits()
