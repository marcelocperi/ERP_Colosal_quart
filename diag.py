path = 'multiMCP/core/templates/admin_risk_dashboard.html'

with open(path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Print lines around the suspect area
for i, line in enumerate(lines, 1):
    if 'if' in line and ('endif' not in line) and ('{% if' in line) and i < 70:
        print(f"Line {i}: {repr(line[:120])}")
    if 'endif' in line and i < 70:
        print(f"Line {i} [endif]: {repr(line[:120])}")
