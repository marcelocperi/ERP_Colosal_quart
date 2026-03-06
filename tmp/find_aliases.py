import re
keywords_set = {"WHERE", "JOIN", "ON", "ORDER", "GROUP", "LIMIT", "AS", "INNER", "LEFT", "RIGHT", "SET", "SELECT", "INSERT", "UPDATE", "DELETE", "AND", "OR", "VALUES"}

with open('c:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP/core/routes.py', 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        matches = re.finditer(r'(?i)\b(?:FROM|JOIN)\s+([a-zA-Z_0-9]+)(?:\s+AS)?\s+([a-zA-Z_0-9]+)\b', line)
        for match in matches:
            t_name = match.group(1).upper()
            t_alias = match.group(2).upper()
            if t_alias not in keywords_set:
                print(f"{i+1}: {line.strip()}")
