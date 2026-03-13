import os
import re

TARGET_DIRS = [
    r'C:\Users\marce\Documents\GitHub\quart\core',
    r'C:\Users\marce\Documents\GitHub\quart\services'
]

def fix_signatures(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    new_lines = []
    changed = False
    
    # Buscamos bloques de funciones
    # Una función necesita ser async si tiene 'await ' o 'async with ' dentro.
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Detectar inicio de función: def nombre(...):
        if line.strip().startswith('def ') and not line.strip().startswith('async def '):
            # Encontrar el bloque de la función (líneas con más indentación)
            indent = len(line) - len(line.lstrip())
            func_body_lines = []
            j = i + 1
            has_async_ops = False
            
            while j < len(lines):
                if lines[j].strip() == '': 
                    func_body_lines.append(lines[j])
                    j += 1
                    continue
                
                curr_indent = len(lines[j]) - len(lines[j].lstrip())
                if curr_indent <= indent:
                    break
                
                if 'await ' in lines[j] or 'async with ' in lines[j]:
                    has_async_ops = True
                
                func_body_lines.append(lines[j])
                j += 1
            
            if has_async_ops:
                # Convertir a async def
                new_lines.append(line.replace('def ', 'async def ', 1))
                changed = True
                # No incrementamos i aquí, las líneas del cuerpo se procesarán normalmente
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)
        i += 1

    if changed:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        return True
    return False

def run_fix():
    count = 0
    for tdir in TARGET_DIRS:
        for root, _, files in os.walk(tdir):
            for file in files:
                if file.endswith('.py'):
                    if fix_signatures(os.path.join(root, file)):
                        print(f"Signaturas corregidas: {file}")
                        count += 1
    print(f"Total archivos con signaturas corregidas: {count}")

if __name__ == "__main__":
    run_fix()
