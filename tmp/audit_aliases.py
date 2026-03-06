
import os
import re

directories = [
    "biblioteca", "compras", "fondos", "contabilidad", "pricing", 
    "produccion", "stock", "utilitarios", "services", "backoffice"
]

# Pattern for SQL aliases: 
# 1. table name followed by space and 1-2 letter alias
# 2. after JOIN or FROM
# Example: JOIN erp_terceros t ON ...
# Example: FROM erp_comprobantes c
alias_pattern = re.compile(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)\s+([a-z]{1,2})(?:\s+ON|\s+WHERE|\s+GROUP|\s+ORDER|\s+LIMIT|\s*["\'\)])', re.IGNORECASE)

# Also pattern for SELECT aliases
# Example: SELECT c.*, t.nombre
select_alias_pattern = re.compile(r'SELECT\s+([a-z]{1,2})\.', re.IGNORECASE)

def scan_file(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
        
    found_aliases = []
    
    # Find FROM/JOIN aliases
    for match in alias_pattern.finditer(content):
        table, alias = match.groups()
        # Find line number
        line_no = content.count('\n', 0, match.start()) + 1
        found_aliases.append(f"Line {line_no}: Table '{table}' shadowed by alias '{alias}'")
        
    # Find usage of aliases in SELECT
    for match in select_alias_pattern.finditer(content):
        alias = match.group(1)
        line_no = content.count('\n', 0, match.start()) + 1
        found_aliases.append(f"Line {line_no}: Alias usage '{alias}.*'")
        
    return found_aliases

results = {}
for root_dir in directories:
    abs_path = os.path.join(os.getcwd(), root_dir)
    if not os.path.exists(abs_path):
        continue
    for root, dirs, files in os.walk(abs_path):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                aliases = scan_file(filepath)
                if aliases:
                    results[filepath] = aliases

if not results:
    print("No short SQL aliases found in targeted modules.")
else:
    for filepath, aliases in results.items():
        print(f"\n--- {filepath} ---")
        for a in aliases[:10]: # Limit output
            print(a)
        if len(aliases) > 10:
            print(f"... and {len(aliases) - 10} more")
