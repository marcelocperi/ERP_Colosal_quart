import os
import re

# Directorios objetivo para la migración
TARGET_DIRS = [
    r'C:\Users\marce\Documents\GitHub\quart\core',
    r'C:\Users\marce\Documents\GitHub\quart\services',
    r'C:\Users\marce\Documents\GitHub\quart\compras',
    r'C:\Users\marce\Documents\GitHub\quart\ventas',
    r'C:\Users\marce\Documents\GitHub\quart\stock',
    r'C:\Users\marce\Documents\GitHub\quart\contabilidad',
    r'C:\Users\marce\Documents\GitHub\quart\fondos',
    r'C:\Users\marce\Documents\GitHub\quart\utilitarios',
    r'C:\Users\marce\Documents\GitHub\quart\pricing',
    r'C:\Users\marce\Documents\GitHub\quart\produccion'
]

# Archivos sueltos
TARGET_FILES = [
    r'C:\Users\marce\Documents\GitHub\quart\app.py',
    r'C:\Users\marce\Documents\GitHub\quart\database.py'
]

def migrate_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error leyendo {filepath}: {e}")
        return False

    original = content

    # 1. Cambiar context manager a asíncrono
    # Buscar: with get_db_cursor(...) as cursor:
    content = re.sub(r'with get_db_cursor\((.*?)\) as (\w+):', r'async with get_db_cursor(\1) as \2:', content)
    
    # 2. Await métodos del cursor
    # cursor.execute, cursor.fetchone, cursor.fetchall, cursor.executemany
    # Pero cuidado de no meter un await si ya existe un await (re-ejecución del script)
    
    # Heurística: \b(NOMBRE_CURSOR)\.execute\( asegurándonos que no haya un await antes.
    # Primero buscamos el nombre del cursor definido en el "async with get_db_cursor(...) as NAME:"
    cursor_names = re.findall(r'async with get_db_cursor\(.*?\) as (\w+):', content)
    unique_cursors = set(cursor_names)
    
    for cname in unique_cursors:
        # cursor.execute -> await cursor.execute
        # Usamos lookbehind negativo para no duplicar await
        content = re.sub(rf'(?<!await\s){cname}\.execute\(', f'await {cname}.execute(', content)
        content = re.sub(rf'(?<!await\s){cname}\.fetchone\(', f'await {cname}.fetchone(', content)
        content = re.sub(rf'(?<!await\s){cname}\.fetchall\(', f'await {cname}.fetchall(', content)
        content = re.sub(rf'(?<!await\s){cname}\.executemany\(', f'await {cname}.executemany(', content)

    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def mass_migrate():
    count = 0
    # Archivos específicos
    for fpath in TARGET_FILES:
        if os.path.exists(fpath):
            if migrate_file(fpath):
                print(f"Migrado: {fpath}")
                count += 1
                
    # Directorios
    for tdir in TARGET_DIRS:
        if not os.path.exists(tdir):
            continue
        for root, _, files in os.walk(tdir):
            for file in files:
                if file.endswith('.py'):
                    fpath = os.path.join(root, file)
                    if migrate_file(fpath):
                        print(f"Migrado: {fpath}")
                        count += 1
                        
    print(f"\nFinalizado. Se migraron {count} archivos a SQL asíncrono.")

if __name__ == "__main__":
    mass_migrate()
