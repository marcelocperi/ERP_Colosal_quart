import re
with open('C:/Users/marce/Documents/GitHub/bibliotecaweb/multiMCP/core/templates/admin_risk_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

ifs = re.findall(r'\{%\s*if', content)
endifs = re.findall(r'\{%\s*endif', content)

print(f"IFs: {len(ifs)}")
print(f"ENDIFs: {len(endifs)}")

# Find which ones are unclosed
lines = content.splitlines()
stack = []
for i, line in enumerate(lines):
    found_if = re.search(r'\{%\s*if\s+(.*?)\s*%\}', line)
    if found_if:
        stack.append((i+1, found_if.group(1)))
    found_endif = re.search(r'\{%\s*endif\s*%\}', line)
    if found_endif:
        if stack:
            stack.pop()
        else:
            print(f"Excess ENDIF at line {i+1}")

for line_num, condition in stack:
    print(f"Unclosed IF at line {line_num}: {condition}")
