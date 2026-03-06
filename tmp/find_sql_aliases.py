
import os
import re

def find_sql_aliases(root_dir):
    # Regex to find FROM or JOIN followed by a table name and a 1-2 char alias
    # Focus on common patterns like 'FROM table t' or 'JOIN table t ON'
    patterns = [
        r'FROM\s+(\w+)\s+([a-z]{1,2})\s+',
        r'JOIN\s+(\w+)\s+([a-z]{1,2})\s+ON',
        r'FROM\s+(\w+)\s+([a-z]{1,2})$',
    ]
    
    results = []
    for root, _, files in os.walk(root_dir):
        if 'venv' in root or '.git' in root or '.gemini' in root:
            continue
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()
                        for i, line in enumerate(lines):
                            for p in patterns:
                                match = re.search(p, line, re.IGNORECASE)
                                if match:
                                    table, alias = match.groups()
                                    # Filter out common misconceptions (e.g., 'as ', 'ON ')
                                    if alias.lower() not in ['as', 'on', 'in', 'is']:
                                        results.append(f"{path}:{i+1}: {line.strip()}")
                                        break
                except:
                    pass
    return results

root = '.'
print("--- POTENTIAL SQL ALIASES ---")
lines = find_sql_aliases(root)
for l in lines:
    print(l)
